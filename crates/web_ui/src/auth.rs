use crate::{
    error::{Error, Result},
    model::{NewUser, User, UserUpdate},
    secrets,
    store::{Store, now},
};
use rusqlite::{OptionalExtension, params};

pub const SESSION_SECONDS: i64 = 8 * 60 * 60;
#[derive(Clone)]
pub(crate) struct Session {
    pub user: User,
    pub csrf_token: String,
    pub token_hash: Vec<u8>,
}
pub(crate) struct Login {
    pub token: String,
    pub session: Session,
}
pub(crate) fn user_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<User> {
    Ok(User {
        id: r.get(0)?,
        owner: r.get(1)?,
        username: r.get(2)?,
        role: r.get(3)?,
        theme: r.get(4)?,
        note: r.get(5)?,
    })
}
fn username(value: &str) -> Result<()> {
    if !(3..=64).contains(&value.len())
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
    {
        return Err(Error::Invalid("账号需要 3–64 位字母、数字或 _ . -"));
    }
    Ok(())
}
fn owner(value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::Invalid("所有者需要 1–128 字节，且不能包含控制字符"));
    }
    Ok(value.to_owned())
}
fn note(value: &str) -> Result<String> {
    let value = value.trim();
    if value.len() > 500 || value.chars().any(char::is_control) {
        return Err(Error::Invalid("备注不能超过 500 字节，且不能包含控制字符"));
    }
    Ok(value.to_owned())
}
pub(crate) fn admin(conn: &rusqlite::Connection, id: i64) -> Result<()> {
    if conn
        .query_row("SELECT role FROM users WHERE id=?1", [id], |r| {
            r.get::<_, String>(0)
        })
        .optional()?
        .as_deref()
        != Some("admin")
    {
        return Err(Error::Forbidden);
    }
    Ok(())
}
impl Store {
    pub fn bootstrap_default_admin(&self) -> Result<()> {
        self.insert_initial_admin("admin", "admin")
    }

    pub fn bootstrap_admin(&self, name: &str, password: &str) -> Result<()> {
        username(name)?;
        secrets::validate_password(password)?;
        self.insert_initial_admin(name, password)
    }

    fn insert_initial_admin(&self, name: &str, password: &str) -> Result<()> {
        let hash = secrets::hash_password(password)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        if tx.query_row("SELECT count(*) FROM users", [], |r| r.get::<_, i64>(0))? != 0 {
            return Err(Error::Conflict("初始管理员已经存在"));
        }
        tx.execute(
            "INSERT INTO users(owner,username,password_hash,role) VALUES(?1,?1,?2,'admin')",
            params![name, hash],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn login(
        &self,
        name: &str,
        password: &str,
        old_token: Option<&str>,
    ) -> Result<Login> {
        if username(name).is_err() || password.len() > 128 {
            return Err(Error::Unauthorized);
        }
        let time = now();
        let stored = {
            let mut conn = self.db()?;
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM login_attempts WHERE window_start < ?1",
                [time - 60],
            )?;
            tx.execute("DELETE FROM sessions WHERE expires_at <= ?1", [time])?;
            for (key, limit) in [("global".to_owned(), 60), (format!("user:{name}"), 6)] {
                let attempts: Option<i64> = tx
                    .query_row(
                        "SELECT attempts FROM login_attempts WHERE name=?1",
                        [&key],
                        |r| r.get(0),
                    )
                    .optional()?;
                if attempts.unwrap_or(0) >= limit {
                    return Err(Error::RateLimited);
                }
                tx.execute("INSERT INTO login_attempts(name,window_start,attempts) VALUES(?1,?2,1) ON CONFLICT(name) DO UPDATE SET attempts=attempts+1",params![key,time])?;
            }
            let entry = tx
                .query_row(
                    "SELECT id,owner,username,role,theme,note,password_hash FROM users WHERE username=?1",
                    [name],
                    |r| Ok((user_row(r)?, r.get::<_, String>(6)?)),
                )
                .optional()?;
            tx.commit()?;
            entry
        };
        let hash = stored
            .as_ref()
            .map(|(_, hash)| hash.as_str())
            .unwrap_or(&self.dummy_hash);
        let valid = secrets::verify_password(password, hash);
        if !valid {
            return Err(Error::Unauthorized);
        }
        let (user, hash) = stored.ok_or(Error::Unauthorized)?;
        let token = secrets::random_token();
        let session = Session {
            user,
            csrf_token: secrets::random_token(),
            token_hash: secrets::digest(&token),
        };
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        let unchanged: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id=?1 AND password_hash=?2)",
            params![session.user.id, hash],
            |r| r.get(0),
        )?;
        if !unchanged {
            return Err(Error::Unauthorized);
        }
        if let Some(old) = old_token {
            tx.execute(
                "DELETE FROM sessions WHERE token_hash=?1",
                [secrets::digest(old)],
            )?;
        }
        tx.execute(
            "DELETE FROM login_attempts WHERE name=?1",
            [format!("user:{name}")],
        )?;
        tx.execute(
            "INSERT INTO sessions(token_hash,user_id,csrf_token,expires_at) VALUES(?1,?2,?3,?4)",
            params![
                session.token_hash,
                session.user.id,
                session.csrf_token,
                time + SESSION_SECONDS
            ],
        )?;
        // Bound abandoned sessions per account.
        tx.execute("DELETE FROM sessions WHERE user_id=?1 AND token_hash NOT IN (SELECT token_hash FROM sessions WHERE user_id=?1 ORDER BY expires_at DESC, rowid DESC LIMIT 20)",[session.user.id])?;
        tx.commit()?;
        Ok(Login { token, session })
    }
    pub(crate) fn session(&self, token: &str) -> Result<Session> {
        if token.len() != 43 {
            return Err(Error::Unauthorized);
        }
        let hash = secrets::digest(token);
        self.db()?.query_row("SELECT u.id,u.owner,u.username,u.role,u.theme,u.note,s.csrf_token FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?1 AND s.expires_at>?2",params![hash,now()],|r|Ok(Session {user:user_row(r)?,csrf_token:r.get(6)?,token_hash:hash.clone()})).optional()?.ok_or(Error::Unauthorized)
    }
    pub(crate) fn logout(&self, hash: Vec<u8>) -> Result<()> {
        self.db()?
            .execute("DELETE FROM sessions WHERE token_hash=?1", [hash])?;
        Ok(())
    }
    pub(crate) fn users(&self, actor: i64) -> Result<Vec<User>> {
        let conn = self.db()?;
        admin(&conn, actor)?;
        Ok(conn
            .prepare("SELECT id,owner,username,role,theme,note FROM users ORDER BY id")?
            .query_map([], user_row)?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub(crate) fn create_user(&self, actor: i64, input: NewUser) -> Result<User> {
        {
            let conn = self.db()?;
            admin(&conn, actor)?;
        }
        username(&input.username)?;
        let owner = owner(&input.owner)?;
        let note = note(&input.note)?;
        secrets::validate_password(&input.password)?;
        if !["admin", "viewer"].contains(&input.role.as_str()) {
            return Err(Error::Invalid("角色只能是 admin 或 viewer"));
        }
        let hash = secrets::hash_password(&input.password)?;
        let conn = self.db()?;
        admin(&conn, actor)?;
        conn.execute(
            "INSERT INTO users(owner,username,password_hash,role,note) VALUES(?1,?2,?3,?4,?5)",
            params![owner, input.username, hash, input.role, note],
        )?;
        Ok(User {
            id: conn.last_insert_rowid(),
            owner,
            username: input.username,
            role: input.role,
            theme: "dark".into(),
            note,
        })
    }
    pub(crate) fn update_user(&self, actor: i64, id: i64, input: UserUpdate) -> Result<User> {
        {
            let conn = self.db()?;
            admin(&conn, actor)?;
        }
        let owner = owner(&input.owner)?;
        let note = note(&input.note)?;
        let password_hash = if let Some(password) = input.password {
            secrets::validate_password(&password)?;
            Some(secrets::hash_password(&password)?)
        } else {
            None
        };
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        let changed = if let Some(hash) = password_hash {
            let changed = tx.execute(
                "UPDATE users SET owner=?1,note=?2,password_hash=?3 WHERE id=?4",
                params![owner, note, hash, id],
            )?;
            tx.execute("DELETE FROM sessions WHERE user_id=?1", [id])?;
            changed
        } else {
            tx.execute(
                "UPDATE users SET owner=?1,note=?2 WHERE id=?3",
                params![owner, note, id],
            )?
        };
        if changed == 0 {
            return Err(Error::NotFound);
        }
        let user = tx.query_row(
            "SELECT id,owner,username,role,theme,note FROM users WHERE id=?1",
            [id],
            user_row,
        )?;
        tx.commit()?;
        Ok(user)
    }
    pub(crate) fn delete_user(&self, actor: i64, id: i64) -> Result<()> {
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        admin(&tx, actor)?;
        let role: String = tx
            .query_row("SELECT role FROM users WHERE id=?1", [id], |r| r.get(0))
            .optional()?
            .ok_or(Error::NotFound)?;
        if role == "admin"
            && tx.query_row("SELECT count(*) FROM users WHERE role='admin'", [], |r| {
                r.get::<_, i64>(0)
            })? <= 1
        {
            return Err(Error::Conflict("不能删除唯一管理员"));
        }
        if actor == id {
            return Err(Error::Conflict("不能删除当前登录账号"));
        }
        tx.execute("DELETE FROM users WHERE id=?1", [id])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn theme(&self, id: i64, value: &str) -> Result<User> {
        if !["dark", "light"].contains(&value) {
            return Err(Error::Invalid("外观只能是 dark 或 light"));
        }
        let conn = self.db()?;
        if conn.execute("UPDATE users SET theme=?1 WHERE id=?2", params![value, id])? == 0 {
            return Err(Error::Unauthorized);
        }
        Ok(conn.query_row(
            "SELECT id,owner,username,role,theme,note FROM users WHERE id=?1",
            [id],
            user_row,
        )?)
    }
    pub(crate) fn change_password(&self, id: i64, old: &str, new: &str) -> Result<()> {
        secrets::validate_password(new)?;
        if old.len() > 128 {
            return Err(Error::Unauthorized);
        }
        let old_hash: String = self
            .db()?
            .query_row("SELECT password_hash FROM users WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or(Error::Unauthorized)?;
        if !secrets::verify_password(old, &old_hash) {
            return Err(Error::Unauthorized);
        }
        let hash = secrets::hash_password(new)?;
        let mut conn = self.db()?;
        let tx = conn.transaction()?;
        if tx.execute(
            "UPDATE users SET password_hash=?1 WHERE id=?2 AND password_hash=?3",
            params![hash, id, old_hash],
        )? != 1
        {
            return Err(Error::Conflict("账号已发生变化，请重新登录"));
        }
        tx.execute("DELETE FROM sessions WHERE user_id=?1", [id])?;
        tx.commit()?;
        Ok(())
    }
}

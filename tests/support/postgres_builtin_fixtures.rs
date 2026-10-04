pub struct BuiltinCase {
    pub name: &'static str,
    pub declaration: &'static str,
    pub expression: &'static str,
}

pub const BUILTINS: &[BuiltinCase] = &[
    BuiltinCase {
        name: "internal_char",
        declaration: "\"char\"",
        expression: "'x'::\"char\"",
    },
    BuiltinCase {
        name: "name_value",
        declaration: "name",
        expression: "'identifier'::name",
    },
    BuiltinCase {
        name: "boolean_value",
        declaration: "boolean",
        expression: "true",
    },
    BuiltinCase {
        name: "smallint_value",
        declaration: "smallint",
        expression: "-32768",
    },
    BuiltinCase {
        name: "integer_value",
        declaration: "integer",
        expression: "2147483647",
    },
    BuiltinCase {
        name: "bigint_value",
        declaration: "bigint",
        expression: "9223372036854775807",
    },
    BuiltinCase {
        name: "numeric_value",
        declaration: "numeric(30,6)",
        expression: "12345678901234567890.123456::numeric(30,6)",
    },
    BuiltinCase {
        name: "negative_scale_numeric",
        declaration: "numeric(10,-2)",
        expression: "123456789::numeric(10,-2)",
    },
    BuiltinCase {
        name: "real_value",
        declaration: "real",
        expression: "'NaN'::real",
    },
    BuiltinCase {
        name: "double_value",
        declaration: "double precision",
        expression: "'-Infinity'::double precision",
    },
    BuiltinCase {
        name: "money_value",
        declaration: "money",
        expression: "'1234.56'::money",
    },
    BuiltinCase {
        name: "text_value",
        declaration: "text",
        expression: "'captured text'",
    },
    BuiltinCase {
        name: "varchar_value",
        declaration: "character varying(255)",
        expression: "'varchar'",
    },
    BuiltinCase {
        name: "bpchar_value",
        declaration: "character(8)",
        expression: "'char'",
    },
    BuiltinCase {
        name: "bytea_value",
        declaration: "bytea",
        expression: "decode('00ff','hex')",
    },
    BuiltinCase {
        name: "bit_value",
        declaration: "bit(8)",
        expression: "B'101'::bit(8)",
    },
    BuiltinCase {
        name: "varbit_value",
        declaration: "bit varying(64)",
        expression: "B'10101'::bit varying(64)",
    },
    BuiltinCase {
        name: "date_value",
        declaration: "date",
        expression: "'0001-01-01 BC'::date",
    },
    BuiltinCase {
        name: "time_value",
        declaration: "time(6) without time zone",
        expression: "'24:00:00'::time(6)",
    },
    BuiltinCase {
        name: "timetz_value",
        declaration: "time(6) with time zone",
        expression: "'12:34:56.123456+05:30'::timetz",
    },
    BuiltinCase {
        name: "timestamp_value",
        declaration: "timestamp(6) without time zone",
        expression: "'0001-01-01 01:02:03 BC'::timestamp(6)",
    },
    BuiltinCase {
        name: "timestamptz_value",
        declaration: "timestamp(6) with time zone",
        expression: "'infinity'::timestamptz",
    },
    BuiltinCase {
        name: "interval_value",
        declaration: "interval",
        expression: "'2 years 3 mons 4 days 05:06:07.123456'::interval",
    },
    BuiltinCase {
        name: "uuid_value",
        declaration: "uuid",
        expression: "'12345678-1234-1234-1234-123456789abc'::uuid",
    },
    BuiltinCase {
        name: "json_value",
        declaration: "json",
        expression: "'{\"n\":1}'::json",
    },
    BuiltinCase {
        name: "jsonb_value",
        declaration: "jsonb",
        expression: "'{\"n\":1}'::jsonb",
    },
    BuiltinCase {
        name: "jsonpath_value",
        declaration: "jsonpath",
        expression: "'$.a'::jsonpath",
    },
    BuiltinCase {
        name: "xml_value",
        declaration: "xml",
        expression: "XMLPARSE(DOCUMENT '<root/>')",
    },
    BuiltinCase {
        name: "point_value",
        declaration: "point",
        expression: "point(1,2)",
    },
    BuiltinCase {
        name: "line_value",
        declaration: "line",
        expression: "line(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "lseg_value",
        declaration: "lseg",
        expression: "lseg(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "box_value",
        declaration: "box",
        expression: "box(point(0,0),point(1,1))",
    },
    BuiltinCase {
        name: "path_value",
        declaration: "path",
        expression: "'[(0,0),(1,1)]'::path",
    },
    BuiltinCase {
        name: "polygon_value",
        declaration: "polygon",
        expression: "'((0,0),(1,1),(2,0))'::polygon",
    },
    BuiltinCase {
        name: "circle_value",
        declaration: "circle",
        expression: "'<(0,0),1>'::circle",
    },
    BuiltinCase {
        name: "cidr_value",
        declaration: "cidr",
        expression: "'192.0.2.0/24'::cidr",
    },
    BuiltinCase {
        name: "inet_value",
        declaration: "inet",
        expression: "'2001:db8::1/64'::inet",
    },
    BuiltinCase {
        name: "macaddr_value",
        declaration: "macaddr",
        expression: "'08:00:2b:01:02:03'::macaddr",
    },
    BuiltinCase {
        name: "macaddr8_value",
        declaration: "macaddr8",
        expression: "'08:00:2b:01:02:03:04:05'::macaddr8",
    },
    BuiltinCase {
        name: "tsvector_value",
        declaration: "tsvector",
        expression: "'fat:1 rat:2'::tsvector",
    },
    BuiltinCase {
        name: "tsquery_value",
        declaration: "tsquery",
        expression: "'fat & rat'::tsquery",
    },
    BuiltinCase {
        name: "oid_value",
        declaration: "oid",
        expression: "42::oid",
    },
    BuiltinCase {
        name: "refcursor_value",
        declaration: "refcursor",
        expression: "'cdc_cursor'::refcursor",
    },
    BuiltinCase {
        name: "aclitem_value",
        declaration: "aclitem",
        expression: "(SELECT unnest(relacl) FROM pg_catalog.pg_class WHERE relacl IS NOT NULL LIMIT 1)",
    },
    BuiltinCase {
        name: "oidvector_value",
        declaration: "oidvector",
        expression: "'1 2'::oidvector",
    },
    BuiltinCase {
        name: "int2vector_value",
        declaration: "int2vector",
        expression: "'1 2'::int2vector",
    },
    BuiltinCase {
        name: "tid_value",
        declaration: "tid",
        expression: "'(0,1)'::tid",
    },
    BuiltinCase {
        name: "xid_value",
        declaration: "xid",
        expression: "'42'::xid",
    },
    BuiltinCase {
        name: "xid8_value",
        declaration: "xid8",
        expression: "'42'::xid8",
    },
    BuiltinCase {
        name: "cid_value",
        declaration: "cid",
        expression: "'42'::cid",
    },
    BuiltinCase {
        name: "lsn_value",
        declaration: "pg_lsn",
        expression: "'0/16B6C50'::pg_lsn",
    },
    BuiltinCase {
        name: "snapshot_value",
        declaration: "pg_snapshot",
        expression: "'1:5:2,3'::pg_snapshot",
    },
    BuiltinCase {
        name: "txid_snapshot_value",
        declaration: "txid_snapshot",
        expression: "'1:5:2,3'::txid_snapshot",
    },
    BuiltinCase {
        name: "regproc_value",
        declaration: "regproc",
        expression: "'abs(integer)'::regprocedure::regproc",
    },
    BuiltinCase {
        name: "regprocedure_value",
        declaration: "regprocedure",
        expression: "'abs(integer)'::regprocedure",
    },
    BuiltinCase {
        name: "regoper_value",
        declaration: "regoper",
        expression: "'=(integer,integer)'::regoperator::regoper",
    },
    BuiltinCase {
        name: "regoperator_value",
        declaration: "regoperator",
        expression: "'=(integer,integer)'::regoperator",
    },
    BuiltinCase {
        name: "regclass_value",
        declaration: "regclass",
        expression: "'pg_class'::regclass",
    },
    BuiltinCase {
        name: "regtype_value",
        declaration: "regtype",
        expression: "'int4'::regtype",
    },
    BuiltinCase {
        name: "regconfig_value",
        declaration: "regconfig",
        expression: "'english'::regconfig",
    },
    BuiltinCase {
        name: "regdictionary_value",
        declaration: "regdictionary",
        expression: "'simple'::regdictionary",
    },
    BuiltinCase {
        name: "regnamespace_value",
        declaration: "regnamespace",
        expression: "'pg_catalog'::regnamespace",
    },
    BuiltinCase {
        name: "regrole_value",
        declaration: "regrole",
        expression: "'postgres'::regrole",
    },
    BuiltinCase {
        name: "regcollation_value",
        declaration: "regcollation",
        expression: "'default'::regcollation",
    },
    BuiltinCase {
        name: "int4range_value",
        declaration: "int4range",
        expression: "int4range(1,5,'[)')",
    },
    BuiltinCase {
        name: "int8range_value",
        declaration: "int8range",
        expression: "int8range(1,5,'[)')",
    },
    BuiltinCase {
        name: "numrange_value",
        declaration: "numrange",
        expression: "numrange(1.25,5.5,'[)')",
    },
    BuiltinCase {
        name: "tsrange_value",
        declaration: "tsrange",
        expression: "tsrange('2020-01-01','2020-01-02','[)')",
    },
    BuiltinCase {
        name: "tstzrange_value",
        declaration: "tstzrange",
        expression: "tstzrange('2020-01-01+00','2020-01-02+00','[)')",
    },
    BuiltinCase {
        name: "daterange_value",
        declaration: "daterange",
        expression: "daterange('2020-01-01','2020-01-05','[)')",
    },
    BuiltinCase {
        name: "int4multirange_value",
        declaration: "int4multirange",
        expression: "'{[1,3),[5,8)}'::int4multirange",
    },
    BuiltinCase {
        name: "int8multirange_value",
        declaration: "int8multirange",
        expression: "'{[1,3),[5,8)}'::int8multirange",
    },
    BuiltinCase {
        name: "nummultirange_value",
        declaration: "nummultirange",
        expression: "'{[1.25,3.5),[5,8)}'::nummultirange",
    },
    BuiltinCase {
        name: "tsmultirange_value",
        declaration: "tsmultirange",
        expression: "'{[\"2020-01-01 00:00:00\",\"2020-01-02 00:00:00\")}'::tsmultirange",
    },
    BuiltinCase {
        name: "tstzmultirange_value",
        declaration: "tstzmultirange",
        expression: "'{[\"2020-01-01 00:00:00+00\",\"2020-01-02 00:00:00+00\")}'::tstzmultirange",
    },
    BuiltinCase {
        name: "datemultirange_value",
        declaration: "datemultirange",
        expression: "'{[2020-01-01,2020-01-05)}'::datemultirange",
    },
];

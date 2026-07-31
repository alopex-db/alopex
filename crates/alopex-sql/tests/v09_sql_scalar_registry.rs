use std::collections::BTreeMap;

use alopex_sql::{
    Span,
    ast::{ddl::VectorMetric, expr::Literal},
    distributed_read::{REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS, REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS},
    planner::{
        typed_expr::{TypedExpr, TypedExprKind},
        types::ResolvedType,
    },
    scalar::{self, Arity, ReturnRule, V09ScalarExecutionStatus},
};

fn typed(ty: ResolvedType) -> TypedExpr {
    TypedExpr::new(TypedExprKind::Literal(Literal::Null), ty, Span::default())
}

fn metric() -> TypedExpr {
    TypedExpr::new(
        TypedExprKind::Literal(Literal::String("cosine".to_owned())),
        ResolvedType::Text,
        Span::default(),
    )
}

fn vector() -> TypedExpr {
    typed(ResolvedType::Vector {
        dimension: 3,
        metric: VectorMetric::Cosine,
    })
}

fn return_type(name: &str, args: &[TypedExpr]) -> ResolvedType {
    let signature = scalar::signature(name).expect("tested matrix row must resolve");
    let arg_types = args
        .iter()
        .map(|arg| arg.resolved_type.clone())
        .collect::<Vec<_>>();
    match &signature.ret {
        ReturnRule::Fixed(ty) => ty.clone(),
        ReturnRule::FromArgs(rule) => rule(&arg_types).expect("accepted argument types return"),
    }
}

#[test]
fn v09_scalar_register_is_complete_unique_and_exactly_arity_fenced() {
    let expected = BTreeMap::from([
        ("vector_similarity", Arity::Exact(3)),
        ("vector_distance", Arity::Exact(3)),
        ("vector_dims", Arity::Exact(1)),
        ("vector_norm", Arity::Exact(1)),
        ("abs", Arity::Exact(1)),
        ("sign", Arity::Exact(1)),
        ("round", Arity::Range(1, 2)),
        ("floor", Arity::Exact(1)),
        ("ceil", Arity::Exact(1)),
        ("ceiling", Arity::Exact(1)),
        ("trunc", Arity::Range(1, 2)),
        ("mod", Arity::Exact(2)),
        ("power", Arity::Exact(2)),
        ("pow", Arity::Exact(2)),
        ("sqrt", Arity::Exact(1)),
        ("exp", Arity::Exact(1)),
        ("ln", Arity::Exact(1)),
        ("log", Arity::Range(1, 2)),
        ("log10", Arity::Exact(1)),
        ("random", Arity::Exact(0)),
        ("sin", Arity::Exact(1)),
        ("cos", Arity::Exact(1)),
        ("tan", Arity::Exact(1)),
        ("asin", Arity::Exact(1)),
        ("acos", Arity::Exact(1)),
        ("atan", Arity::Exact(1)),
        ("atan2", Arity::Exact(2)),
        ("degrees", Arity::Exact(1)),
        ("radians", Arity::Exact(1)),
        ("pi", Arity::Exact(0)),
        ("sha256", Arity::Exact(1)),
        ("md5", Arity::Exact(1)),
        ("simhash", Arity::Exact(1)),
        ("hamming_distance", Arity::Exact(2)),
        ("gen_random_uuid", Arity::Exact(0)),
        ("uuidv7", Arity::Exact(0)),
        ("hex", Arity::Exact(1)),
        ("unhex", Arity::Exact(1)),
        ("encode", Arity::Exact(2)),
        ("decode", Arity::Exact(2)),
        ("length", Arity::Exact(1)),
        ("char_length", Arity::Exact(1)),
        ("octet_length", Arity::Exact(1)),
        ("upper", Arity::Exact(1)),
        ("lower", Arity::Exact(1)),
        ("initcap", Arity::Exact(1)),
        ("substr", Arity::Range(2, 3)),
        ("left", Arity::Exact(2)),
        ("right", Arity::Exact(2)),
        ("trim", Arity::Range(1, 2)),
        ("ltrim", Arity::Range(1, 2)),
        ("rtrim", Arity::Range(1, 2)),
        ("replace", Arity::Exact(3)),
        ("instr", Arity::Exact(2)),
        ("strpos", Arity::Exact(2)),
        ("concat", Arity::Variadic(0)),
        ("concat_ws", Arity::Variadic(1)),
        ("repeat", Arity::Exact(2)),
        ("reverse", Arity::Exact(1)),
        ("lpad", Arity::Range(2, 3)),
        ("rpad", Arity::Range(2, 3)),
        ("split_part", Arity::Exact(3)),
        ("regexp_replace", Arity::Exact(3)),
        ("regexp_match", Arity::Exact(2)),
        ("regexp_matches", Arity::Range(2, 3)),
        ("coalesce", Arity::Variadic(1)),
        ("nullif", Arity::Exact(2)),
        ("ifnull", Arity::Exact(2)),
        ("iif", Arity::Exact(3)),
        ("greatest", Arity::Variadic(1)),
        ("least", Arity::Variadic(1)),
        ("typeof", Arity::Exact(1)),
        ("pg_typeof", Arity::Exact(1)),
        ("quote", Arity::Exact(1)),
        ("memory_stats", Arity::Exact(0)),
        ("io_stats", Arity::Exact(0)),
        ("clear_cache", Arity::Exact(0)),
    ]);
    let actual = scalar::signatures()
        .iter()
        .map(|signature| (signature.name, signature.arity))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        actual, expected,
        "missing, unknown, duplicate, or arity drift"
    );
    assert!(scalar::signature("v09_unknown_function").is_none());
}

#[test]
fn v09_numeric_vector_hash_register_is_closed_and_matches_catalog_metadata() {
    use scalar::V09ScalarExecutionStatus::{Distributed, LocalOnly};

    let expected = BTreeMap::from([
        (
            "SQL-F-V01",
            (
                "vector_similarity",
                Arity::Exact(3),
                LocalOnly,
                scalar::PURE_META,
            ),
        ),
        (
            "SQL-F-V02",
            (
                "vector_distance",
                Arity::Exact(3),
                LocalOnly,
                scalar::PURE_META,
            ),
        ),
        (
            "SQL-F-V03",
            ("vector_dims", Arity::Exact(1), LocalOnly, scalar::PURE_META),
        ),
        (
            "SQL-F-V04",
            ("vector_norm", Arity::Exact(1), LocalOnly, scalar::PURE_META),
        ),
        (
            "SQL-F-N01",
            ("abs", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N02",
            ("sign", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N03",
            ("round", Arity::Range(1, 2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N04",
            ("floor", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N05",
            ("ceil", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N06",
            ("ceiling", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N07",
            ("trunc", Arity::Range(1, 2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N08",
            ("mod", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N09",
            ("power", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N10",
            ("pow", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N11",
            ("sqrt", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N12",
            ("exp", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N13",
            ("ln", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N14",
            ("log", Arity::Range(1, 2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N15",
            ("log10", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N16",
            ("random", Arity::Exact(0), LocalOnly, scalar::RANDOM_META),
        ),
        (
            "SQL-F-N17",
            ("sin", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N18",
            ("cos", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N19",
            ("tan", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N20",
            ("asin", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N21",
            ("acos", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N22",
            ("atan", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N23",
            ("atan2", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N24",
            ("degrees", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N25",
            ("radians", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-N26",
            ("pi", Arity::Exact(0), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H01",
            ("sha256", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H02",
            ("md5", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H03",
            ("simhash", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H04",
            (
                "hamming_distance",
                Arity::Exact(2),
                Distributed,
                scalar::PURE_META,
            ),
        ),
        (
            "SQL-F-H05",
            (
                "gen_random_uuid",
                Arity::Exact(0),
                LocalOnly,
                scalar::RANDOM_META,
            ),
        ),
        (
            "SQL-F-H06",
            ("uuidv7", Arity::Exact(0), LocalOnly, scalar::RANDOM_META),
        ),
        (
            "SQL-F-H07",
            ("hex", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H08",
            ("unhex", Arity::Exact(1), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H09",
            ("encode", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
        (
            "SQL-F-H10",
            ("decode", Arity::Exact(2), Distributed, scalar::PURE_META),
        ),
    ]);

    let entries = scalar::v09_numeric_vector_hash_scalar_register();
    let actual = entries
        .iter()
        .map(|entry| {
            (
                entry.id,
                (
                    entry.name,
                    entry.arity,
                    entry.execution_status,
                    entry.metadata,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected, "every Phase 4.7 row must be fixed once");
    assert_eq!(
        entries.len(),
        actual.len(),
        "row IDs must not be duplicated"
    );
    assert_eq!(entries.len(), 40, "Phase 4.7 must not abbreviate its rows");

    for entry in entries {
        let signature = scalar::signature(entry.name).expect("matrix row must resolve locally");
        assert_eq!(signature.arity, entry.arity, "{} arity drift", entry.id);
        assert_eq!(
            signature.meta, entry.metadata,
            "{} metadata drift",
            entry.id
        );
        match entry.execution_status {
            Distributed => assert!(
                REMOTE_DETERMINISTIC_SCALAR_FUNCTIONS.contains(&entry.name),
                "{} must stay in the closed distributed catalog",
                entry.id
            ),
            LocalOnly => assert!(
                REMOTE_LOCAL_ONLY_SCALAR_FUNCTIONS.contains(&entry.name),
                "{} must remain local-only",
                entry.id
            ),
        }
    }

    assert_eq!(
        scalar::v09_numeric_vector_hash_scalar("RANDOM").map(|entry| entry.execution_status),
        Some(V09ScalarExecutionStatus::LocalOnly)
    );
    assert!(scalar::v09_numeric_vector_hash_scalar("not_registered").is_none());
}

#[test]
fn v09_numeric_vector_hash_rows_preserve_type_and_null_contracts() {
    for entry in scalar::v09_numeric_vector_hash_scalar_register() {
        let (args, expected_return) = match entry.name {
            "vector_similarity" | "vector_distance" => {
                (vec![vector(), vector(), metric()], ResolvedType::Double)
            }
            "vector_dims" => (vec![typed(ResolvedType::Null)], ResolvedType::Integer),
            "vector_norm" => (vec![typed(ResolvedType::Null)], ResolvedType::Double),
            "random" | "pi" => (Vec::new(), ResolvedType::Double),
            "sign" => (vec![typed(ResolvedType::Null)], ResolvedType::Integer),
            "abs" | "round" | "floor" | "ceil" | "ceiling" | "trunc" | "mod" => {
                let count = match entry.arity {
                    Arity::Exact(count) | Arity::Range(_, count) => count,
                    Arity::Variadic(_) => unreachable!("Phase 4.7 has no variadic rows"),
                };
                (vec![typed(ResolvedType::Null); count], ResolvedType::Null)
            }
            "power" | "pow" | "sqrt" | "exp" | "ln" | "log" | "log10" | "sin" | "cos" | "tan"
            | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" => {
                let count = match entry.arity {
                    Arity::Exact(count) | Arity::Range(_, count) => count,
                    Arity::Variadic(_) => unreachable!("Phase 4.7 has no variadic rows"),
                };
                (vec![typed(ResolvedType::Null); count], ResolvedType::Double)
            }
            "sha256" => (vec![typed(ResolvedType::Null)], ResolvedType::Blob),
            "md5" | "hex" => (vec![typed(ResolvedType::Null)], ResolvedType::Text),
            "simhash" => (vec![typed(ResolvedType::Null)], ResolvedType::BigInt),
            "hamming_distance" => (
                vec![typed(ResolvedType::Null), typed(ResolvedType::Null)],
                ResolvedType::Integer,
            ),
            "gen_random_uuid" | "uuidv7" => (Vec::new(), ResolvedType::Text),
            "unhex" => (vec![typed(ResolvedType::Null)], ResolvedType::Blob),
            "decode" => (
                vec![typed(ResolvedType::Null), typed(ResolvedType::Null)],
                ResolvedType::Blob,
            ),
            "encode" => (
                vec![typed(ResolvedType::Null), typed(ResolvedType::Null)],
                ResolvedType::Text,
            ),
            name => panic!("uncovered Phase 4.7 type/null row: {name}"),
        };

        let signature = scalar::signature(entry.name).expect("matrix row must resolve locally");
        signature
            .arity
            .validate(entry.name, args.len(), Span::default())
            .expect("test fixture must use the exact registered arity");
        (signature.check)(&args).unwrap_or_else(|error| {
            panic!(
                "{} must preserve its accepted type/null contract: {error}",
                entry.id
            )
        });
        assert_eq!(
            return_type(entry.name, &args),
            expected_return,
            "{} return-type/null contract drift",
            entry.id
        );
    }
}

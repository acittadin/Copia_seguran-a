from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F


def init_spark(project_root: Path) -> SparkSession:
    java_home = Path("/usr/lib/jvm/java-17-openjdk-amd64")
    if java_home.exists():
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")
    else:
        raise RuntimeError("JDK 17 não encontrado em /usr/lib/jvm/java-17-openjdk-amd64. Instale o OpenJDK 17 para rodar o Spark localmente.")

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    workspace_dir = (project_root / "spark-warehouse").resolve()
    (project_root / "bronze").mkdir(exist_ok=True)
    (project_root / "silver").mkdir(exist_ok=True)
    (project_root / "gold").mkdir(exist_ok=True)
    workspace_dir.mkdir(exist_ok=True)

    try:
        current = SparkSession.getActiveSession()
        if current is not None:
            current.stop()
    except Exception:
        pass

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName("Projeto_Medalhao_Local_Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(workspace_dir))
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_csv_local(spark: SparkSession, landing_dir: Path, file_name: str) -> DataFrame:
    target_name = file_name.strip().lower().replace(".csv", "")
    for item in sorted(landing_dir.iterdir()):
        if not item.is_file():
            continue
        item_name = item.name.lower().replace(":zone.identifier", "")
        item_base = item_name.rsplit(".csv", 1)[0] if item_name.endswith(".csv") else item_name
        if item_base == target_name:
            df = (
                spark.read.option("inferSchema", "true")
                .option("header", "true")
                .csv(str(item))
            )
            fixed_cols = [c.replace("\ufeff", "") for c in df.columns]
            return df.toDF(*fixed_cols)
    raise FileNotFoundError(f"Arquivo '{file_name}' não encontrado em {landing_dir}")


def apply_name_rules(colname: str) -> str:
    n = colname.strip().replace("\ufeff", "").upper().replace(" ", "_")
    n = n.replace("ID_", "IDENTIFIER_")
    n = n.replace("ID", "IDENTIFIER")
    return n


def safe_drop(df: DataFrame, cols: list[str]) -> DataFrame:
    existing = set(df.columns)
    to_drop = [c for c in cols if c in existing]
    return df.drop(*to_drop) if to_drop else df


def drop_audit_columns(df: DataFrame) -> DataFrame:
    cols_to_remove = [c for c in df.columns if c.upper() in {"DATA_HORA_SILVER", "NOME_TABELA", "DATA_HORA_BRONZE", "NOME_ARQUIVO"}]
    return df.drop(*cols_to_remove) if cols_to_remove else df


def first_matching_column(df: DataFrame, candidates: list[str]) -> str | None:
    upper_map = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        if candidate.upper() in upper_map:
            return upper_map[candidate.upper()]
    return None


def build_dimension(df: DataFrame, id_candidates: list[str], name_candidates: list[str], id_alias: str, name_alias: str) -> DataFrame:
    id_col = first_matching_column(df, id_candidates)
    name_col = first_matching_column(df, name_candidates)
    if id_col is None:
        raise ValueError(f"Nenhuma coluna de identificador encontrada. Candidatos: {id_candidates}")

    selected = [F.col(id_col).alias(id_alias)]
    if name_col is not None:
        selected.append(F.col(name_col).alias(name_alias))
    else:
        selected.append(F.lit(None).cast("string").alias(name_alias))
    return df.select(*selected)


def bronze_stage(spark: SparkSession, landing_dir: Path, bronze_dir: Path) -> None:
    tables = {
        "teacher": read_csv_local(spark, landing_dir, "teacher"),
        "classes": read_csv_local(spark, landing_dir, "classes"),
        "students": read_csv_local(spark, landing_dir, "students"),
        "subjects": read_csv_local(spark, landing_dir, "subjects"),
        "grades": read_csv_local(spark, landing_dir, "grades"),
    }

    for table_name, df in tables.items():
        df = df.withColumn("data_hora_bronze", F.current_timestamp()).withColumn("nome_arquivo", F.lit(table_name))
        df.write.format("delta").mode("overwrite").save(str(bronze_dir / table_name))
        print(f"Bronze salva: {table_name}")


def silver_stage(spark: SparkSession, bronze_dir: Path, silver_dir: Path) -> None:
    dataframes = {
        "teacher": spark.read.format("delta").load(str(bronze_dir / "teacher")),
        "classes": spark.read.format("delta").load(str(bronze_dir / "classes")),
        "students": spark.read.format("delta").load(str(bronze_dir / "students")),
        "subjects": spark.read.format("delta").load(str(bronze_dir / "subjects")),
        "grades": spark.read.format("delta").load(str(bronze_dir / "grades")),
    }

    for table_name, df in dataframes.items():
        df = df.withColumn("data_hora_silver", F.current_timestamp()).withColumn("nome_tabela", F.lit(table_name))
        new_cols = [apply_name_rules(c) for c in df.columns]
        df = df.toDF(*new_cols)
        df = safe_drop(df, ["DATA_HORA_BRONZE", "NOME_ARQUIVO"])
        df.write.format("delta").mode("overwrite").save(str(silver_dir / table_name))
        print(f"Silver salva: {table_name}")


def gold_stage(spark: SparkSession, silver_dir: Path, gold_dir: Path) -> None:
    df_students = spark.read.format("delta").load(str(silver_dir / "students"))
    df_teacher = spark.read.format("delta").load(str(silver_dir / "teacher"))
    df_classes = spark.read.format("delta").load(str(silver_dir / "classes"))
    df_subjects = spark.read.format("delta").load(str(silver_dir / "subjects"))
    df_grades = spark.read.format("delta").load(str(silver_dir / "grades"))

    df_students = drop_audit_columns(df_students)
    df_teacher = drop_audit_columns(df_teacher)
    df_classes = drop_audit_columns(df_classes)
    df_subjects = drop_audit_columns(df_subjects)
    df_grades = drop_audit_columns(df_grades)

    dim_student = build_dimension(
        df_students,
        ["IDENTIFIER", "STUDENT_IDENTIFIER", "ID_STUDENT", "STUDENT_ID", "IDENTIFIER_STUDENT"],
        ["NAME", "NOME", "NOME_COMPLETO", "NOME_ESTUDANTE"],
        "student_id",
        "student_name",
    )
    dim_teacher = build_dimension(
        df_teacher,
        ["IDENTIFIER", "TEACHER_IDENTIFIER", "ID_TEACHER", "TEACHER_ID", "IDENTIFIER_TEACHER"],
        ["NAME", "NOME", "NOME_PROFESSOR", "NOME_COMPLETO"],
        "teacher_id",
        "teacher_name",
    )
    dim_class = build_dimension(
        df_classes,
        ["IDENTIFIER", "CLASS_IDENTIFIER", "ID_CLASS", "CLASS_ID", "IDENTIFIER_CLASS"],
        ["NAME", "NOME", "NOME_TURMA", "DESCRICAO"],
        "class_id",
        "class_name",
    )
    dim_subject = build_dimension(
        df_subjects,
        ["IDENTIFIER", "SUBJECT_IDENTIFIER", "ID_SUBJECT", "SUBJECT_ID", "IDENTIFIER_SUBJECT"],
        ["NAME", "NOME", "NOME_DISCIPLINA", "DESCRICAO"],
        "subject_id",
        "subject_name",
    )

    student_fk = first_matching_column(df_grades, ["STUDENT_IDENTIFIER", "IDENTIFIER", "ID_STUDENT", "STUDENT_ID", "IDENTIFIER_STUDENT"])
    teacher_fk = first_matching_column(df_grades, ["TEACHER_IDENTIFIER", "IDENTIFIER", "ID_TEACHER", "TEACHER_ID", "IDENTIFIER_TEACHER"])
    class_fk = first_matching_column(df_grades, ["CLASS_IDENTIFIER", "IDENTIFIER", "ID_CLASS", "CLASS_ID", "IDENTIFIER_CLASS"])
    subject_fk = first_matching_column(df_grades, ["SUBJECT_IDENTIFIER", "IDENTIFIER", "ID_SUBJECT", "SUBJECT_ID", "IDENTIFIER_SUBJECT"])
    grade_value = first_matching_column(df_grades, ["GRADE", "NOTA", "GRADE_VALUE", "NOTA_FINAL", "VALOR_NOTA"])

    if None in {student_fk, teacher_fk, class_fk, subject_fk, grade_value}:
        raise ValueError("Uma ou mais colunas essenciais da tabela grades não foram encontradas.")

    fact_grades = df_grades.select(
        F.col(student_fk).alias("student_id"),
        F.col(teacher_fk).alias("teacher_id"),
        F.col(class_fk).alias("class_id"),
        F.col(subject_fk).alias("subject_id"),
        F.col(grade_value).cast("double").alias("grade_value"),
    )

    dim_student.write.format("delta").mode("overwrite").save(str(gold_dir / "dim_student"))
    dim_teacher.write.format("delta").mode("overwrite").save(str(gold_dir / "dim_teacher"))
    dim_class.write.format("delta").mode("overwrite").save(str(gold_dir / "dim_class"))
    dim_subject.write.format("delta").mode("overwrite").save(str(gold_dir / "dim_subject"))
    fact_grades.write.format("delta").mode("overwrite").save(str(gold_dir / "fact_grades"))

    print("Gold gravado com sucesso")


def main() -> None:
    project_root = Path(__file__).resolve().parent
    landing_dir = project_root / "landing" / "dados"
    bronze_dir = project_root / "bronze"
    silver_dir = project_root / "silver"
    gold_dir = project_root / "gold"

    spark = init_spark(project_root)
    bronze_stage(spark, landing_dir, bronze_dir)
    silver_stage(spark, bronze_dir, silver_dir)
    gold_stage(spark, silver_dir, gold_dir)

    print("Pipeline concluído com sucesso.")
    print("Estruturas geradas:", sorted(p.name for p in gold_dir.iterdir() if p.is_dir()))


if __name__ == "__main__":
    main()

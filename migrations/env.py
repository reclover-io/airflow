from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool, MetaData
from alembic import context

# Import Metadata from your project
from components.constants import DB_CONNECTION # แก้ไขเป็น path ที่แท้จริง

# กำหนด Metadata สำหรับ autogenerate
metadata = MetaData(bind=DB_CONNECTION)
target_metadata = metadata

# Alembic Config object
config = context.config

# Interpret the config file for Python logging.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

def include_object(object, name, type_, reflected, compare_to):
    """
    กำหนดให้ Alembic เพิกเฉยตารางที่ไม่ต้องการจัดการ
    - object: SQLAlchemy schema object
    - name: ชื่อของ schema object
    - type_: ประเภทของ schema object เช่น "table"
    - reflected: บอกว่า schema object นี้ถูกสะท้อนจากฐานข้อมูลหรือไม่
    - compare_to: schema object ที่สร้างจาก target_metadata (ถ้ามี)
    """
    if type_ == "table" and name in [
        "ab_permission",
        "ab_permission_view",
        "ab_permission_view_role",
        "ab_register_user",
        "ab_role",
        "ab_user",
        "ab_user_role",
        "ab_view_menu",
        "alembic_version",
        "callback_request",
        "celery_taskmeta",
        "celery_tasksetmeta",
        "connection",
        "dag",
        "dag_code",
        "dag_owner_attributes",
        "dag_pickle",
        "dag_run",
        "dag_run_note",
        "dag_schedule_dataset_reference",
        "dag_tag",
        "dag_warning",
        "dagrun_dataset_event",
        "dataset",
        "dataset_dag_run_queue",
        "dataset_event",
        "import_error",
        "job",
        "log",
        "log_template",
        "rendered_task_instance_fields",
        "serialized_dag",
        "session",
        "sla_miss",
        "slot_pool",
        "task_fail",
        "task_instance",
        "task_instance_note",
        "task_map",
        "task_outlet_dataset_reference",
        "task_reschedule",
        "trigger",
        "variable",
        "xcom",
    ]:
        return False  # เพิกเฉยตารางเหล่านี้
    return True  # จัดการ object อื่น ๆ เช่น `batch_states`




def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,  # ตรวจสอบประเภทของ column สำหรับ autogenerate
            compare_server_default=True,  # ตรวจสอบค่า default ของ column สำหรับ autogenerate
            include_object=include_object,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

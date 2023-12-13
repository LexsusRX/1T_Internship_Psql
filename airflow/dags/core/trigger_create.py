import logging
import psycopg2
from connect_settings import conn
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    format='%(threadName)s %(name)s %(levelname)s: %(message)s',
    level=logging.INFO
)


class TriggerCreator:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor()
        self.front_schema = 'core_schema'
        self.path = '/opt/airflow/dags/core/update_meta.py'

    def create_meta_update_trigger(self):
        try:
            # func_create = f"""
            # CREATE OR REPLACE FUNCTION core_schema_trigger()
            # RETURNS event_trigger AS $$
            # BEGIN
            #    IF (TG_TABLE_SCHEMA={self.front_schema}) THEN
            #         EXECUTE 'python {self.path}';
            #    END IF;
            # END;
            # $$ LANGUAGE plpgsql;
            # """
            """Download plsh or plpythonu"""
            self.cur.execute("""CREATE EXTENSION plsh;""")
            func_create = f"""
            CREATE OR REPLACE FUNCTION core_schema_trigger()
            RETURNS event_trigger 
            LANGUAGE plsh
            AS $$
            #!/bin/sh
            if (PLSH_TG_TABLE_SCHEMA = {self.front_schema}); then
                echo "starting update_meta.py"
                python3 {self.path}
                echo "process ended"
            fi
            $$;
            """
            self.cur.execute(func_create)
            trigger_create = f"""
            CREATE EVENT TRIGGER core_schema_event_trigger
            ON ddl_command_end
            EXECUTE FUNCTION core_schema_trigger();
            """
            # WHEN TAG IN ('ALTER TABLE', 'CREATE TABLE', 'DROP TABLE',
            #              'CREATE SCHEMA', 'DROP SCHEMA')

            self.cur.execute(trigger_create)
            logging.info("function and trigger created successfully")
            self.conn.commit()
            self.conn.close()
        except Exception as e:
            logging.error(f"Error while creating function or trigger to update meta: {e}")
            self.conn.rollback()
            self.conn.close()

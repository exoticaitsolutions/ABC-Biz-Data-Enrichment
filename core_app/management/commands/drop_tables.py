from django.core.management.base import BaseCommand
from django.db import connection

class Command(BaseCommand):
    help = 'Drops all tables from the database'

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cursor.execute(
                "SELECT CONCAT('DROP TABLE IF EXISTS `', table_name, '`;') "
                "FROM information_schema.tables "
                "WHERE table_schema = DATABASE();"
            )
            for result in cursor.fetchall():
                cursor.execute(result[0])
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        self.stdout.write(self.style.SUCCESS("All tables dropped successfully."))

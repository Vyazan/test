class IntegrationDB:

    def __init__(self, conn_str: str, integration_name: str):
        self.integration_name = integration_name
        self.__conn_str = conn_str

        self.__connection = None
        self.__cursor = None

    def connect(self):
        # Создаем подключение
        self.__connection = psycopg2.connect(dsn=self.__conn_str)
        # Создаем курсор для выполнения запросов БД
        self.__cursor = self.__connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def close(self):
        self.__cursor.close()
        self.__connection.close()

    def add_integration_secrets(self, client_id: str, client_secret: str):
        sql = "SELECT * FROM integrations WHERE integration_name = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name,))
        row = self.__cursor.fetchone()
        self.close()

        if row:
            # Секреты от интеграции уже записаны в БД
            return None

        sql = 'INSERT INTO integrations (integration_name, client_id, client_secret) VALUES (%s, %s, %s)'
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, client_id, client_secret))
        self.__connection.commit()
        self.close()

    def add_integration(self, account_id: int, subdomain: str, client_id: str,
                        client_secret: str,
                        redirect_uri: str, last_query_at: int, refresh_token: str, access_token: str,
                        expire_access_token: int, expire_refresh_token: int, free_query: int,
                        limit_query: int = 5):

        sql = "SELECT * FROM integrations WHERE integration_name = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name,))
        row = self.__cursor.fetchone()
        self.close()
        if not row:
            # Запись не найдена. Создаем
            sql = "INSERT INTO integrations (integration_name, account_id, subdomain, client_id, client_secret, " \
                  "redirect_uri, last_query_at, refresh_token, access_token, expire_access_token, " \
                  "expire_refresh_token, free_query, limit_query) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.connect()
            self.__cursor.execute(sql, (self.integration_name, account_id, subdomain, client_id, client_secret,
                                        redirect_uri, last_query_at, refresh_token, access_token, expire_access_token,
                                        expire_refresh_token, free_query, limit_query))
            self.__connection.commit()
            self.close()

        else:
            # Запись найдена.
            # Проверяем, не устарели ли наши данные
            if not row["expire_access_token"] is None and row["expire_access_token"] > expire_access_token:
                # В БД находятся более свежие данные. Перезаписывать не будем
                return

            # Обновляем запись
            sql = "UPDATE integrations SET integration_name = %s, account_id = %s, subdomain = %s, client_id = %s, " \
                  "client_secret = %s, redirect_uri = %s, last_query_at = %s, refresh_token = %s, access_token = %s, " \
                  "expire_access_token = %s, expire_refresh_token = %s, free_query = %s, limit_query = %s " \
                  "WHERE id = %s"
            self.connect()
            self.__cursor.execute(sql, (self.integration_name, account_id, subdomain, client_id, client_secret,
                                        redirect_uri, last_query_at, refresh_token, access_token, expire_access_token,
                                        expire_refresh_token, free_query, limit_query, row["id"]))
            self.__connection.commit()
            self.close()

    def get_integration(self) -> Optional[IntegrationTable]:
        sql = "SELECT * FROM integrations WHERE integration_name = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name,))
        row = self.__cursor.fetchone()
        self.close()
        if not row:
            return None

        res = IntegrationTable(id=row["id"],
                               integration_name=row["integration_name"],
                               account_id=row["account_id"],
                               subdomain=row["subdomain"],
                               client_id=row["client_id"],
                               client_secret=row["client_secret"],
                               redirect_uri=row["redirect_uri"],
                               last_query_at=row["last_query_at"],
                               refresh_token=row["refresh_token"],
                               access_token=row["access_token"],
                               expire_access_token=row["expire_access_token"],
                               expire_refresh_token=row["expire_refresh_token"],
                               free_query=row["free_query"],
                               limit_query=row["limit_query"])

        return res

    def delete_integration(self):
        integration = self.get_integration()
        if not integration:
            return
        sql = "DELETE FROM integrations WHERE id = %s"
        self.connect()
        self.__cursor.execute(sql, (integration.id,))
        self.__connection.commit()
        self.close()

    def add_integration_data(self, data_name: str, data: str):
        sql = "INSERT INTO integrations_data (integration_name, data_name, data) VALUES (%s, %s, %s)"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, data_name, data))
        self.__connection.commit()
        self.close()

    def get_integration_data(self, data_name: str, sort_direction: str = "ASC") -> IntegrationData:
        sql = f"SELECT * FROM integrations_data WHERE integration_name = %s AND data_name = %s ORDER BY created_at " \
              f"{sort_direction} LIMIT 1"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, data_name))
        row = self.__cursor.fetchone()
        self.close()
        if not row:
            return None

        res = IntegrationData(id=row['id'], integration_name=row['integration_name'], created_at=row['created_at'],
                              data_name=row['data_name'], data=row['data'])
        return res

    def get_integrations_data(self, data_name: Optional[str] = None) -> List[IntegrationData]:
        sql = f"SELECT * FROM integrations_data WHERE integration_name = %s AND data_name = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, data_name))
        rows = self.__cursor.fetchall()
        self.close()
        if not rows:
            return None
        res = []
        for row in rows:
            res.append(IntegrationData(id=row['id'], integration_name=row['integration_name'],
                                       created_at=row['created_at'], data_name=row['data_name'], data=row['data']))
        return res

    def delete_integration_data(self, integration_data_id: int):
        sql = "DELETE FROM integrations_data WHERE id = %s"
        self.connect()
        self.__cursor.execute(sql, (integration_data_id,))
        self.__connection.commit()
        self.close()

    def set_setting(self, key: str, value: str):
        sql = "SELECT * FROM settings WHERE integration_name = %s AND key = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, key))
        row = self.__cursor.fetchone()
        self.close()
        if not row:
            # Записи нет. Создаем.
            sql = "INSERT INTO settings (integration_name, key, value) VALUES (%s, %s, %s)"
            self.connect()
            self.__cursor.execute(sql, (self.integration_name, key, value))
            self.__connection.commit()
            self.close()

            return
        else:
            # Запись есть. Обновляем
            sql = "UPDATE settings SET value = %s WHERE id = %s"
            self.connect()
            self.__cursor.execute(sql, (value, row['id']))
            self.__connection.commit()
            self.close()
            return

    def get_setting(self, key: str) -> Optional[SettingsTable]:
        sql = "SELECT * FROM settings WHERE integration_name = %s AND key = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, key))
        row = self.__cursor.fetchone()
        self.close()

        if not row:
            return None

        res = SettingsTable(id=row['id'], integration_name=row['integration_name'], key=row['key'], value=row['value'])
        return res

    def delete_setting(self, key: str):
        sql = "DELETE FROM settings WHERE integration_name = %s AND key = %s"
        self.connect()
        self.__cursor.execute(sql, (self.integration_name, key))
        self.__connection.commit()
        self.close()

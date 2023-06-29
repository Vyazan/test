class IntegrationDB:

    def __init__(self, conn_str: str, integration_name: str):
        self.integration_name = integration_name
        self.__conn_str = conn_str
        self.session = None


    def connect(self):
        engine = create_engine(self.__conn_str, poolclass=NullPool)
        Base.metadata.create_all(bind=engine)


        # Свяжем engine с метаданными класса Base,
        # чтобы декларативы могли получить доступ через экземпляр Session
        Base.metadata.bind = engine
        Session = sessionmaker(bind=engine)
        self.session = Session()


    def check_connection(self):
        if not self.session or not self.session.is_active:
            self.connect()

    def close(self):
        self.session.close()

    def add_integration_secrets(self, client_id: str, client_secret: str):
        self.check_connection()
        row = self.session.query(IntegrationTable).filter(
            IntegrationTable.integration_name == self.integration_name).one_or_none()
        if row:
            # Секреты от интеграции уже записаны в БД
            self.close()
            return None
        row = IntegrationTable(integration_name=self.integration_name, client_id=client_id,
                               client_secret=client_secret)
        self.session.add(row)
        self.session.commit()
        self.close()

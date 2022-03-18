# get document path
data_path = input("Enter document path pls:")
# get database type (dialect)
db_type = input("Enter database type: ")
# get database connector
db_connector = input("Enter database connector: ")
# get username
db_user = input(f"Enter username: ")
# get password
db_pass = input("Enter password: ")
# get connection hostname
db_host_name = input(f"Enter connection hostname: ")
# get connection port
db_port = input(f"Enter connection port: ")
# get the database name
db_name = input("Enter database name: ")


# define function for connection and data injection into given database
def data_injection(doc_path: str, db_type: str, db_connector: str, user_name: str, password: str, host_name: str, db_addr: str, db_name: str):
    """
    Function to inject data into a given RDBMS (relational database management system)

    Parameters
    ----------
    doc_path: str
        Document's path to parse.
    db_type: str 
        Specifies the kind (dialect) of database we're connecting to. SQLAlchemy can interface with all mainstream flavors of relational databases. Here are some databases examples:
            MySQL: mysql
            PostgreSQL: postgresql
            SQLite: sqlite
            Oracle (ugh): oracle
            Microsoft SQL (slightly less exasperated "ugh"): mssql
    db_connector: str
        To manage your database connections, SQLAlchemy leverages whichever Python database connection library you chose to use. Here are the libraries recommended per dialect:
            MySQL: pymysql, mysqldb
            PostgreSQL: psycopg2, pg8000
            SQLite: (none needed)
            Oracle: cx_oracle
            Microsoft SQL: pymssql, pyodbc
    user_name: str
        Name of the user to connect with.
    password: str
        User's password.
    host_name: str
        Name or IP address of the server host.
    db_addr: str
        TCP/IP port.
    db_name: str 
        Name of the database to define if not already exists.
    """

    """Unit testing"""
    from IPython.core import display as ICD

    def check_connexion(engine_obj: object):
        """
        Unit testing function to check connection status with chosen database

        Parameters
        ---------
        engine: object
            Defined engine to establish connection with given parameters

        Returns
        -------
        Raising AssertionError if connection failed
        """
        from sqlalchemy.exc import SQLAlchemyError

        try:
            if not database_exists(engine_obj.url):
                create_database(engine_obj.url)
                print("Connection succeeded")
            else:
                engine.connect()
                print("Connection succeeded")
        except SQLAlchemyError as err:
            print(f"Connection failed because of: {err.__cause__}")

    """Extract tables from pdf document given url"""
    import tabula

    data_dfs = tabula.read_pdf(
        doc_path, pages="all", multiple_tables=True)

    # define fucntion to process data before injection into chosen database
    def clean_pdf(tables: list):
        """
        Function to clean tables parsed from the document with framed tables

        Parameters
        ----------
        tables: list
            Array containing document's parsed tables 
        """
        import pickle

        # define appropriate parsing method for given document
        if tables[-1].shape[1] < 3:
            # define appropraite parsing method
            tables = tabula.read_pdf(
                doc_path, pages="all", multiple_tables=True, stream=True)

            # drop empty rows
            tables.dropna(how="all", inplace=True)

            # define tables array length (number of parsed tables)
            length = (len(tables))

            # rename if column 'Unnamed'
            for i in range(length):
                if 'Unnamed: 0' in tables[i].columns:
                    tables[i] = tables[i].rename(
                        columns={'Unnamed: 0': 'colonne_1'})

            # drop column 'Unamed' if dtype int
            for i in range(length):
                if 'colonne_1' in tables[i].columns:
                    if tables[i]['colonne_1'].dtypes == 'int64':
                        tables[i] = tables[i].drop(['colonne_1'], axis=1)

            # merge parsed tables if with columns matching
            table_nb = []
            for i in reversed(range(length)):
                if i not in table_nb:
                    table_nb.append(i)
                for e in reversed(range(length)):
                    if (i != e) and (e not in table_nb) and (pickle.dumps(tables[i].columns) == pickle.dumps(tables[e].columns)):
                        tables[i] = (tables[i].append(tables[e])
                                     ).reset_index(drop=True)
                        del tables[e]
        elif tables[0].shape[0] > 0:
            tables = tabula.read_pdf(
                doc_path, pages="all", multiple_tables=True, lattice=True)
            tables = [table.drop("Unnamed: 0", axis=1, errors="ignore").dropna(
                how="all") for table in tables]

        # return processed tables
        return tables

    # instantiate preprocessing function to define parsed tables variable
    data_dfs = clean_pdf(tables=data_dfs)

    """Create database connection"""
    from sqlalchemy import create_engine
    from sqlalchemy_utils import database_exists, create_database

    db_uri = f"{db_type}+{db_connector}://{user_name}:{password}@{host_name}:{db_addr}/{db_name}"

    engine = create_engine(
        db_uri,
        connect_args={
            "ssl": {
                "ssl_ca": "/home/gord/client-ssl/ca.pem",
                "ssl_cert": "/home/gord/client-ssl/client-cert.pem",
                "ssl_key": "/home/gord/client-ssl/client-key.pem"
            }},
        echo=True,
    )

    check_connexion(engine)  # unit testing db connection status

    """Data injection"""
    for i, df in enumerate(data_dfs):
        if df.shape[0] > 0:
            if data_dfs[i-1].shape[0] == 0:
                df_title = data_dfs[i-1].columns[0]
                df.to_sql(df_title, con=engine,
                          if_exists='replace', index=False)
            else:
                df.to_sql(f"table_{i}", con=engine,
                          if_exists='replace', index=False)
        else:
            continue


data_injection(data_path, db_type, db_connector, db_user,
               db_pass, db_host_name, db_port, db_name)

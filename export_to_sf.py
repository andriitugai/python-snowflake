import snowflake as sf

def sf_connect(
        sf_user = 'andriitugai',
        sf_account = 'ce85120.west-europe.azure',
        sf_passwd = 'im_Happy1109'
):
    """
    Creates connection to SnowFlake
    :param sf_user:
    :param sf_account:
    :param sf_passwd:
    :return:
    """
    if sf_passwd == '':
        import getpass
        sf_passwd = getpass.getpass('Password:')

    # Test the connection to Snowflake by retrieving the version number
    from sqlalchemy import create_engine
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/'.format(
            user=sf_user,
            password=sf_passwd,
            account=sf_account
        )
    )
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print("Snowflake Version: " + results[0])
        connection.close()
    except:
        print('Connection failed, check credentials')
        return
    finally:
        engine.dispose()
    connection = sf.connector.connect(
        user=sf_user,
        password=sf_passwd,
        account=sf_account,
    )
    print('Connection established')
    return connection

def main():
    conn = sf_connect()


if __name__ == '__main__':
    main()



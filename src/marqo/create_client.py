import marqo


MARQO_URL = "http://gk4k0ckgck04g04ow8w08wws.100.71.51.35.sslip.io"

def create_client():
    mq = marqo.Client(url=MARQO_URL)

    print("Connected to Marqo")

    return mq

import botocore.session
from util import log


class AuthenticationError(Exception):
    pass


def cognito_aut(funcao):
    def wrapper(*arg, **kwargs):
        session = botocore.session.Session()
        client = session.create_client('cognito-idp', region_name='us-east-1')

        try:
            response = client.admin_initiate_auth(
                UserPoolId='us-east-1_zG7dE8XaZ',
                ClientId='22peq1l17j1rit149vlghmo8ru',
                AuthFlow='ADMIN_USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': 'qa@domrock.ai',
                    'PASSWORD': 'Dr.12345'
                }
            )
        except Exception as e:
            log.createLogger(cognito_aut.__name__).critical(f'Contate o suporte sobre o seguinte erro: {e}')
            raise AuthenticationError("Contate o suporte") from e

        funcao(*arg, **kwargs)
    return wrapper

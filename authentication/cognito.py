import boto3
from util import log


class AuthenticationError(Exception):
    pass


def cognito_aut(config: dict):
    def inner_aut(funcao):
        def wrapper(*arg, **kwargs):
            username = config.get('username')
            password = config.get('password')
            if username and password:
                client = boto3.client('cognito-idp', region_name='us-east-1')

                try:
                    response = client.initiate_auth(
                        ClientId='571qh22ddklt9eotmminkkv10a',
                        AuthFlow='USER_PASSWORD_AUTH',
                        AuthParameters={
                            'USERNAME': username,
                            'PASSWORD': password
                        }
                    )
                except Exception as e:
                    log.createLogger(cognito_aut.__name__).critical(f'Contate o suporte sobre o seguinte erro: {e}')
                    raise AuthenticationError("Contate o suporte") from e

            funcao(*arg, **kwargs)
        return wrapper
    return inner_aut

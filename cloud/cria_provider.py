from cloud.local_provider import Local
from cloud.aws_provider import AWS
from cloud.provider import Provider


class CriaProvider:
    _options = {
        'local': Local,
        'aws': AWS
    }

    def create_provider(self, option: str) -> Provider:
        provider = self._options.get(option)
        if not provider:
            raise NotImplementedError(f'o provider {option} nao esta disponivel')
        return provider()

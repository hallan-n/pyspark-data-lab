from abc import ABC, abstractmethod

class LayerFlow(ABC):
    @abstractmethod
    def run(self):
        """Executa o processamento principal da camada."""
        pass

    @abstractmethod
    def show(self):
        """Exibe informações ou amostras dos dados processados pela camada."""
        pass
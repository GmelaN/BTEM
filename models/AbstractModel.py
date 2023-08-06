from abc import ABC, abstractmethod

class BasicModel(ABC):
    @abstractmethod
    def predict(self) -> bool:
        pass

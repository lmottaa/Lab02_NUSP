from datetime import datetime, timedelta
import time

def calculate_latency(start_time: datetime, end_time: datetime) -> timedelta:
    """
    Calcula a latência (diferença de tempo) entre dois pontos.

    Args:
        start_time (datetime): O instante inicial (primeira chamada).
        end_time (datetime): O instante final (segunda chamada).

    Returns:
        timedelta: Objeto nativo do Python representando a duração exata 
                   entre o início e o fim.
    """
    # O operador de subtração entre objetos datetime retorna um timedelta
    latency: timedelta = end_time - start_time
    return latency
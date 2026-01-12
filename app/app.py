import asyncio
import httpx
import logging
import os
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Конфигурация из переменных окружения ---
DGS_URL = os.getenv("DGS_URL", "https://eta.api.2gis.ru/v2/points/directions") # Эндпоинт для запроса данных
AVG_SPEED_KMH = float(os.getenv("AVG_SPEED", "26"))  # Средняя скорость в км/ч
ETA_LIMIT_MINUTES = int(os.getenv("ETA_LIMIT", "30")) # Максимальное ETA для возврата, в минутах
POLL_INTERVAL_DEFAULT = int(os.getenv("POLL_INTERVAL", "15")) # Интервал опроса по умолчанию в секундах
MISS_LIMIT = int(os.getenv("MISS_LIMIT", "5")) # Количество пропусков, после которого возвращается null

# Преобразуем лимит в секунды для внутренних расчетов
ETA_LIMIT_SECONDS = ETA_LIMIT_MINUTES * 60

app = FastAPI()

# --- Глобальные переменные для кеширования ---
cache_data = {}

def get_cache_key(route_name: str, direction_id: str) -> str:
    """Генерирует ключ для кеша на основе параметров, влияющих на запрос к API."""
    return f"{route_name}_{direction_id}"

async def fetch_2gis_data(direction_ids: list[str]) -> Optional[dict]:
    """Асинхронно получает сырые данные от 2GIS API."""
    headers = {
        'origin': 'https://2gis.ru',
        'referer': 'https://2gis.ru/',
        'content-type': 'application/json',
        'user-agent': 'Mozilla/5.0'
    }
    payload = {
        "directions": direction_ids,
        "type": "online5",
        "immersive": False
    }

    try:
        logger.debug(f"Отправка запроса к {DGS_URL} с направлениями {direction_ids}")
        async with httpx.AsyncClient() as client:
            response = await client.post(DGS_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"Получены данные от 2GIS, количество устройств: {len(data.get('devices', []))}")
            return data
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP ошибка при запросе к 2GIS: {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=502, detail=f"Ошибка от внешнего API: {e.response.status_code}")
    except httpx.RequestError as e:
        logger.error(f"Ошибка запроса к 2GIS: {str(e)}")
        raise HTTPException(status_code=502, detail="Не удалось подключиться к внешнему API")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при запросе к 2GIS: {str(e)}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

def calculate_eta(bus_device: dict, target_stop_part: float) -> Optional[float]:
    """
    Рассчитывает ETA для одного автобуса.
    Возвращает время в секундах или None, если автобус не подходит.
    """
    try:
        # Проверяем тип транспорта
        if bus_device.get("transport_type") != "bus":
            logger.debug(f"Устройство {bus_device.get('device_id')} не является автобусом, пропускаем.")
            return None

        # Извлекаем необходимые параметры
        route_name = bus_device.get("route_name")
        direction_id = bus_device.get("direction_id")
        direction_length = bus_device.get("direction_length") # в метрах
        geometry_way_part = bus_device.get("geometry_way_part") # доля пройденного пути

        # Проверяем наличие обязательных данных
        if not all(v is not None for v in [route_name, direction_length, geometry_way_part]):
             logger.debug(f"Устройство {bus_device.get('device_id')} не содержит всех необходимых данных для расчета.")
             return None

        # Рассчитываем пройденное расстояние автобуса
        bus_moved_part = direction_length * geometry_way_part # в метрах

        # Рассчитываем расстояние до нашей остановки
        stop_way_distance = direction_length * target_stop_part # в метрах

        # Проверяем, проехал ли автобус остановку
        if bus_moved_part >= stop_way_distance:
            logger.debug(f"Автобус {bus_device.get('device_id')} (маршрут {route_name}) уже проехал остановку на part {target_stop_part}. Пропускаем.")
            return None

        # Рассчитываем оставшееся расстояние до остановки
        remaining_distance_meters = stop_way_distance - bus_moved_part # в метрах

        # Преобразуем среднюю скорость в м/с
        avg_speed_mps = AVG_SPEED_KMH * 1000.0 / 3600.0

        # Рассчитываем ETA в секундах
        eta_seconds = remaining_distance_meters / avg_speed_mps

        logger.debug(
            f"Рассчитано ETA для автобуса {bus_device.get('device_id')} "
            f"(маршрут {route_name}, направление {direction_id}): {eta_seconds:.2f} сек."
        )
        return eta_seconds

    except (TypeError, ValueError) as e:
        logger.warning(f"Ошибка при обработке данных автобуса {bus_device.get('device_id')}: {e}")
        return None

# --- GET эндпоинт ---
@app.get("/eta")
async def get_eta(
    route_name: str = Query(..., description="Название маршрута, например '168'"),
    direction_id: str = Query(..., description="ID направления, например '4504746941846382'"),
    stop_part: float = Query(..., description="Доля пути до остановки, например 0.4440"),
    # Добавим query параметр для указания, в каких единицах возвращать ETA (sec или min), по умолчанию min
    unit: str = Query("min", description="Единицы измерения ETA: 'sec' для секунд, 'min' для минут")
):
    cache_key = get_cache_key(route_name, direction_id)
    current_time = asyncio.get_event_loop().time()

    # Проверяем кеш
    cached_item = cache_data.get(cache_key)
    cache_expired = not cached_item or (current_time - cached_item['time']) > POLL_INTERVAL_DEFAULT

    if cache_expired:
        logger.info(f"Запрашиваем новые данные от 2GIS для направления {direction_id} (маршрут {route_name}).")
        raw_data = await fetch_2gis_data([direction_id])
        # Обновляем кеш
        cache_data[cache_key] = {'data': raw_data, 'time': current_time, 'miss_count': 0} # Сброс счетчика при обновлении
    else:
        logger.info(f"Используем закешированные данные для направления {direction_id} (маршрут {route_name}).")

    # Получаем данные из кеша
    cached_item = cache_data[cache_key]
    last_response_data = cached_item['data']

    if not last_response_data or "devices" not in last_response_data:
         logger.error("Полученные данные от 2GIS не содержат списка устройств.")
         raise HTTPException(status_code=502, detail="Некорректные данные от внешнего API")

    devices = last_response_data["devices"]
    logger.info(f"Обрабатываем {len(devices)} устройств из кеша для маршрута {route_name}.")

    candidate_etas = []
    for device in devices:
        # Проверяем, соответствует ли устройство нашему запросу (маршрут и направление)
        if device.get("route_name") == route_name and device.get("direction_id") == direction_id:
            eta = calculate_eta(device, stop_part)
            if eta is not None:
                 candidate_etas.append(eta)

    if not candidate_etas:
        # Увеличиваем счетчик пропусков в кеше для этого ключа
        cached_item['miss_count'] += 1
        miss_count = cached_item['miss_count']
        logger.info(f"Не найдено подходящих автобусов для маршрута {route_name}, направления {direction_id}. "
                     f"Счетчик пропусков: {miss_count}/{MISS_LIMIT}")
        if miss_count >= MISS_LIMIT:
             logger.info(f"Превышено количество пропусков ({MISS_LIMIT}) для {route_name}/{direction_id}. Возвращаем null.")
             # Сбросим счетчик, если он превышен, чтобы при следующем появлении автобуса он снова начал считать
             cached_item['miss_count'] = 0
             return {"eta": None}
        else:
            logger.info(f"Нет подходящих автобусов. Счетчик пропусков: {cached_item['miss_count']}/{MISS_LIMIT}. Возвращаем '>{ETA_LIMIT_MINUTES}'.")
            # Возвращаем строку, если лимит не достигнут
            result_value = f">{ETA_LIMIT_MINUTES}"
            # Не сбрасываем счетчик здесь, он сбросится при следующем успешном нахождении кандидата
            return {"eta": result_value}


    # Если есть кандидаты, сбрасываем счетчик и обрабатываем результаты
    min_eta_seconds = min(candidate_etas)
    min_eta_minutes = min_eta_seconds / 60.0
    cached_item['miss_count'] = 0 # Сброс счетчика при успешном нахождении кандидатов
    logger.info(f"Найдено {len(candidate_etas)} подходящих автобусов. Минимальное ETA: {min_eta_minutes:.2f} мин.")

    # Применяем лимит ETA: если минимальное ETA больше лимита, возвращаем строку
    if min_eta_seconds > ETA_LIMIT_SECONDS:
        logger.info(f"Минимальное ETA ({min_eta_minutes:.2f} мин.) превышает лимит ({ETA_LIMIT_MINUTES} мин.). Возвращаем строку.")
        result_value = f">{ETA_LIMIT_MINUTES}м"
        return {"eta": result_value}

    # Преобразуем в минуты, если нужно
    if unit.lower() == "min":
        # Округляем до 1 знака после запятой для минут, чтобы не было 0.0
        result_value = round(min_eta_seconds / 60.0, 1)
        # Если результат 0.0 или отрицательный (из-за округления), покажем как 0
        if result_value <= 0:
            result_value = 0
    else: # по умолчанию секунды
        result_value = round(min_eta_seconds)

    logger.info(f"Возвращаем рассчитанное ETA: {result_value} ({unit}).")
    return {"eta": result_value}

# --- Запуск сервера ---
if __name__ == "__main__":
    import uvicorn
    # Этот блок позволяет запустить сервер напрямую из Python (например, для отладки).
    # В Docker используется команда из Dockerfile.
    uvicorn.run(app, host="0.0.0.0", port=8000)


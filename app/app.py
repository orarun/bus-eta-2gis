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
DGS_URL = os.getenv("DGS_URL", "https://eta.api.2gis.ru/v2/points/directions")
AVG_SPEED_KMH = float(os.getenv("AVG_SPEED", "26"))
ETA_LIMIT_MINUTES = int(os.getenv("ETA_LIMIT", "30"))
POLL_INTERVAL_DEFAULT = int(os.getenv("POLL_INTERVAL", "15"))
MISS_LIMIT = int(os.getenv("MISS_LIMIT", "5"))

ETA_LIMIT_SECONDS = ETA_LIMIT_MINUTES * 60

app = FastAPI()

# --- Кеширование ---
cache_data = {}

def get_cache_key(route_name: str, direction_id: str) -> str:
    # Генерируем ключ для кеша на основе параметров, влияющих на запрос к API
    return f"{route_name}_{direction_id}"

async def fetch_2gis_data(direction_ids: list[str]) -> Optional[dict]:
    # Асинхронно получаем сырые данные от 2GIS API
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
    try:
        if bus_device.get("transport_type") != "bus":
            return None

        route_name = bus_device.get("route_name")
        direction_id = bus_device.get("direction_id")
        direction_length = bus_device.get("direction_length")
        geometry_way_part = bus_device.get("geometry_way_part")

        # Проверяем наличие обязательных данных
        if not all(v is not None for v in [route_name, direction_length, geometry_way_part]):
             return None

        # Рассчитываем пройденное расстояние автобуса
        bus_moved_part = direction_length * geometry_way_part
        # Рассчитываем расстояние до нашей остановки
        stop_way_distance = direction_length * target_stop_part

        # Проверяем, проехал ли автобус остановку
        if bus_moved_part >= stop_way_distance:
            return None

        # Рассчитываем оставшееся расстояние до остановки
        remaining_distance_meters = stop_way_distance - bus_moved_part
        avg_speed_mps = AVG_SPEED_KMH * 1000.0 / 3600.0
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
    unit: str = Query("min", description="Единицы измерения ETA: 'sec' для секунд, 'min' для минут"),
    return_all_sorted: bool = Query(False, description="Вернуть отсортированный список всех ETAs (в указанных единицах)")
):
    cache_key = get_cache_key(route_name, direction_id)
    current_time = asyncio.get_event_loop().time()

    # Проверяем кеш
    cached_item = cache_data.get(cache_key)
    cache_expired = not cached_item or (current_time - cached_item['time']) > POLL_INTERVAL_DEFAULT

    if cache_expired:
        logger.info(f"Запрашиваем новые данные от 2GIS для направления {direction_id} (маршрут {route_name}).")
        raw_data = await fetch_2gis_data([direction_id])
        cache_data[cache_key] = {'data': raw_data, 'time': current_time, 'miss_count': 0}
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
        if device.get("route_name") == route_name and device.get("direction_id") == direction_id:
            eta = calculate_eta(device, stop_part)
            if eta is not None:
                 candidate_etas.append(eta)

    # --- Обработка результатов ---
    if not candidate_etas:
        cached_item['miss_count'] += 1
        miss_count = cached_item['miss_count']
        logger.info(f"Не найдено подходящих автобусов для маршрута {route_name}, направления {direction_id}. "
                     f"Счетчик пропусков: {miss_count}/{MISS_LIMIT}")
        if miss_count >= MISS_LIMIT:
             logger.info(f"Превышено количество пропусков ({MISS_LIMIT}) для {route_name}/{direction_id}. Возвращаем null.")
             cached_item['miss_count'] = 0
             return {"eta": None}
        else:
            logger.info(f"Нет подходящих автобусов. Счетчик пропусков: {cached_item['miss_count']}/{MISS_LIMIT}. Возвращаем '>{ETA_LIMIT_MINUTES}'.")
            result_value = f">{ETA_LIMIT_MINUTES}"
            return {"eta": result_value}

    # Сброс счетчика при нахождении кандидатов
    cached_item['miss_count'] = 0

    # Сортируем ETAs
    sorted_etas_seconds = sorted(candidate_etas)
    logger.info(f"Найдено {len(sorted_etas_seconds)} подходящих автобусов. Отсортированные ETAs (сек): {[round(e, 2) for e in sorted_etas_seconds]}")

    # --- Измененная логика преобразования и округления ---
    def convert_and_limit(eta_sec_val):
        if unit.lower() == "min":
            # --- Основное изменение: отбрасываем дробную часть ---
            val = int(eta_sec_val / 60.0)
            # Если результат отрицательный (из-за точности вычислений или округления), делаем 0
            if val < 0:
                val = 0
            limit_check_val = eta_sec_val # проверяем в секундах
        else: # sec
            val = round(eta_sec_val)
            limit_check_val = val
        if limit_check_val > ETA_LIMIT_SECONDS:
            return f">{ETA_LIMIT_MINUTES}м"
        return val

    # Если нужно вернуть все ETAs
    if return_all_sorted:
        # Преобразуем все ETAs в нужные единицы с помощью новой функции
        sorted_etas_converted = [convert_and_limit(e) for e in sorted_etas_seconds]

        logger.info(f"Возвращаем отсортированный список ETAs (округленные): {sorted_etas_converted}")
        return {"etas": sorted_etas_converted}

    # --- Обработка одиночного ETA ---
    min_eta_seconds = sorted_etas_seconds[0]
    result_value = convert_and_limit(min_eta_seconds)

    logger.info(f"Возвращаем рассчитанное и округленное ETA: {result_value} ({unit}).")
    return {"eta": result_value}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

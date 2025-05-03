import asyncio
import websockets
import json

point = [
    {
        "latitude": 47.3979733,
        "longitude": 8.5461655,
        "altitude": 10,
        "type": 2,
        "mission": -1,
        "velocity": 1.0,
    },
    {
        "latitude": 47.3979733,
        "longitude": 8.5465655,
        "altitude": 10,
        "type": 2,
        "mission": -1,
        "velocity": 1.0,
    },
    {
        "latitude": 47.3975733,
        "longitude": 8.5461655,
        "altitude": 10,
        "type": 2,
        "mission": -1,
        "velocity": 1.0,
    },
    {
        "latitude": 47.3975733,
        "longitude": 8.5461655,
        "altitude": 10,
        "type": 2,
        "mission": -1,
        "velocity": 1.0,
    },
    {
        "latitude": 47.3979733,
        "longitude": 8.5461655,
        "altitude": 10,
        "type": 2,
        "mission": -1,
        "velocity": 1.0,
    },
]


async def send_waypoint(websocket):
    await websocket.send(json.dumps(point))


async def recv_message(websocket):
    try:
        async for message in websocket:
            print(f"Received: {message}")
    except websockets.ConnectionClosed:
        print("Connection closed")


async def main():
    async with websockets.connect("ws://192.168.57.123:8765") as websocket:
        send_task = asyncio.create_task(send_waypoint(websocket))
        receive_task = asyncio.create_task(recv_message(websocket))
        await asyncio.gather(send_task, receive_task)


asyncio.run(main())

import asyncio
from app.services.agent import ask_agent

async def main():
    r = await ask_agent("show me weekly revenue trend")
    print(r)

if __name__ == '__main__':
    asyncio.run(main())


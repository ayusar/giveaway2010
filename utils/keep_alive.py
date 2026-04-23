"""
Keep-alive pinger — prevents Render free tier from sleeping.
Pings the health endpoint every 14 minutes.
"""
import asyncio
import logging

logger = logging.getLogger(__name__)
_domain = None


def set_domain(domain: str):
    global _domain
    _domain = domain


async def keep_alive_loop():
    await asyncio.sleep(60)  # Wait for server to start
    while True:
        try:
            if _domain:
                import aiohttp
                url = f"https://{_domain}/health"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            logger.info(f"💓 Keep-alive ping OK ({_domain})")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")
        await asyncio.sleep(840)  # 14 minutes

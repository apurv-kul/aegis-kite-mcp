
# ─────────────────────────────────────────────
# Demo  __main__
# ─────────────────────────────────────────────

import asyncio
import logging
import sys
import time

from src.event_bus.publisher import MarketDataPublisher
from src.event_bus.consumer import MarketDataConsumer

# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("aegis.demo")

async def run_demo():
    log.info("═" * 60)
    log.info("Project Aegis — Local Redis Event Bus Demo")
    log.info("═" * 60)

    DEMO_STREAM = "aegis.demo.ticks"

    # 1. Initialize Publisher
    publisher = MarketDataPublisher(stream_name=DEMO_STREAM, maxlen=1000)

    # Note how the 'instrument' now matches the strict TradeInstrument contract
    mock_ticks = [
        {
            "instrument": {
                "symbol": "NIFTY26JUN23000CE",
                "underlying_symbol": "NIFTY",
                "expiry_date": "2026-06-25",
                "strike_price": 23000.0,
                "option_type": "CE"
            },
            "spot_price": 23150.40,
            "bid_price": 149.90,
            "ask_price": 150.60,
            "volume": 12450,
            "implied_volatility": 14.3,
            "delta": 0.42,
            "source": "demo-script"
        },
        {
            "instrument": {
                "symbol": "NIFTY26JUN23000PE",
                "underlying_symbol": "NIFTY",
                "expiry_date": "2026-06-25",
                "strike_price": 23000.0,
                "option_type": "PE"
            },
            "spot_price": 23150.40,
            "bid_price": 95.20,
            "ask_price": 95.80,
            "volume": 9800,
            "implied_volatility": 16.1,
            "delta": -0.58,
            "source": "demo-script"
        }
    ]

    log.info("\n── Publishing Mock Ticks ──")
    for raw_data in mock_ticks:
        msg_id = publisher.publish_raw(raw_data)
        log.info(f"  ✓ Published {raw_data['instrument']['symbol']} | ID: {msg_id}")
        time.sleep(0.1) 

    publisher.close()

    # 2. Initialize Consumer
    log.info("\n── Starting Consumer ──")
    consumer = MarketDataConsumer(
        stream_name=DEMO_STREAM,
        group_name="aegis.demo.group",
        consumer_name="local-worker-1"
    )

    received = 0
    async for msg_id, tick in consumer.listen_for_ticks():
        received += 1
        log.info(
            f"  ✓ Consumed ID: {msg_id} | "
            f"Symbol: {tick.instrument.symbol} | "
            f"Spot: {tick.spot_price} | "
            f"IV: {tick.implied_volatility}"
        )
        
        # Acknowledge the message so it drops from the Pending Entry List (PEL)
        await consumer.acknowledge_tick(msg_id)

        if received >= len(mock_ticks):
            break 

    log.info("\n── Validating State ──")
    pending = await consumer.pending_count()
    log.info(f"  Messages still pending in queue: {pending} (Should be 0)")
    
    await consumer.close()
    log.info("Demo complete. Pipeline is solid.")

if __name__ == "__main__":
    # Ensure our python path can find the src directory just like the MCP server
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent))
    
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\nDemo aborted via terminal.")
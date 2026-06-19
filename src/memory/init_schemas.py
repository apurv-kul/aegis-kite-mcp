import logging
import sys
from neo4j import GraphDatabase
import psycopg2

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("aegis.memory.init")

# ─────────────────────────────────────────────
# 1. Neo4j Initialization (Market Structure)
# ─────────────────────────────────────────────
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "aegis_secure_graph_123")

def init_neo4j():
    log.info("Initializing Neo4j Graph Constraints...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    
    # Cypher queries to enforce uniqueness and create indexes
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Index) REQUIRE i.symbol IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stock) REQUIRE s.symbol IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sec:Sector) REQUIRE sec.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (o:OptionContract) REQUIRE o.symbol IS UNIQUE"
    ]
    
    try:
        with driver.session() as session:
            for query in constraints:
                session.run(query)
                log.info(f"Executed: {query}")
            
            # Create a foundational NIFTY node as a starting point
            session.run("""
                MERGE (i:Index {symbol: 'NIFTY', name: 'Nifty 50'})
                MERGE (b:Index {symbol: 'BANKNIFTY', name: 'Nifty Bank'})
            """)
        log.info("✅ Neo4j Market Structure Initialized.")
    except Exception as e:
        log.error(f"Neo4j Init Failed: {e}")
    finally:
        driver.close()


# ─────────────────────────────────────────────
# 2. PostgreSQL / TimescaleDB Initialization
# ─────────────────────────────────────────────
PG_DSN = "postgresql://aegis_admin:local_secure_password_123@localhost:5432/aegis_market_data"

def init_postgres():
    log.info("Initializing PostgreSQL (Timescale + pgvector)...")
    
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Enable the vector extension
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        
        # 2. Create the semantic memory tables (1536 dims for OpenAI embeddings)
        tables = [
            """
            CREATE TABLE IF NOT EXISTS trade_post_mortems (
                id BIGSERIAL PRIMARY KEY,
                trade_id VARCHAR(50) UNIQUE,
                analysis TEXT,
                embedding vector(1536),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS market_regimes (
                id BIGSERIAL PRIMARY KEY,
                date DATE UNIQUE,
                regime_description TEXT,
                embedding vector(1536),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        ]
        
        for table_query in tables:
            cursor.execute(table_query)
            
        log.info("✅ PostgreSQL Vector Memory Initialized.")
        
    except Exception as e:
        log.error(f"Postgres Init Failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    init_neo4j()
    init_postgres()
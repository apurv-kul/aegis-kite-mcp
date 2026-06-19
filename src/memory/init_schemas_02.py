import logging
import sys
import psycopg2

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger("aegis.memory.migration_02")

# ─────────────────────────────────────────────
# PostgreSQL Migration: Enterprise Trading Journal
# ─────────────────────────────────────────────
PG_DSN = "postgresql://aegis_admin:local_secure_password_123@localhost:5432/aegis_market_data"

def migrate_postgres():
    log.info("Starting Schema Migration 02: Upgrading to Enterprise Trading Journal...")
    
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Drop the outdated V1 table if it exists
        cursor.execute("DROP TABLE IF EXISTS trade_post_mortems;")
        log.info("Dropped legacy 'trade_post_mortems' table.")
        
        # 2. Create the V2 Comprehensive Trading Journal Schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trading_journal (
                id BIGSERIAL PRIMARY KEY,
                trade_id VARCHAR(50) UNIQUE NOT NULL,
                date_opened DATE NOT NULL DEFAULT CURRENT_DATE,
                date_closed DATE,
                underlying VARCHAR(20) NOT NULL,
                expiry DATE NOT NULL,
                strategy_type VARCHAR(50) NOT NULL,
                
                -- Part 1: Financial Core Data
                net_premium NUMERIC(12, 2) NOT NULL,
                margin_blocked NUMERIC(12, 2) NOT NULL,
                max_risk NUMERIC(12, 2) NOT NULL,
                
                -- Part 2: Execution Plan
                target_exit NUMERIC(12, 2),
                stop_loss_exit NUMERIC(12, 2),
                time_exit_trigger TEXT,
                
                -- Part 3: The Thesis (Structured + Semantic text)
                technical_thesis TEXT,
                options_thesis TEXT,
                
                -- Part 4: Post-Trade Review & Stress-Test
                exit_reason TEXT,
                final_pnl NUMERIC(12, 2),
                stress_rating INT CHECK (stress_rating BETWEEN 1 AND 5),
                lessons_learned TEXT,
                
                -- Vector embedding for GraphRAG semantic lookups
                journal_embedding vector(1536),
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        log.info("Created highly-structured 'trading_journal' table with vector support.")
        
        log.info("✅ Migration 02 Complete. The database is ready for Phase 2 agents.")
        
    except Exception as e:
        log.error(f"Migration Failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    migrate_postgres()
"""Test script for complete aggregation pipeline."""
from pathlib import Path
from python.processors.aggregator import KPIAggregator

db_path = Path('data/duckdb/gaming.db')

print("🚀 TESTING COMPLETE AGGREGATION PIPELINE\n")
print("=" * 60)

with KPIAggregator(db_path=db_path) as aggregator:
    # Test daily aggregations
    print("\n📊 [1/3] Testing DAILY aggregations...")
    print("-" * 60)
    aggregator.create_daily_kpis()
    
    # Check daily results
    steam_daily = aggregator.db_manager.query("SELECT COUNT(*) as count FROM steam_daily_kpis")
    twitch_daily = aggregator.db_manager.query("SELECT COUNT(*) as count FROM twitch_daily_kpis")
    igdb_daily = aggregator.db_manager.query("SELECT COUNT(*) as count FROM igdb_ratings_snapshot")
    
    print(f"✅ steam_daily_kpis: {steam_daily['count'][0]} records")
    print(f"✅ twitch_daily_kpis: {twitch_daily['count'][0]} records")
    print(f"✅ igdb_ratings_snapshot: {igdb_daily['count'][0]} records")
    
    # Test weekly aggregations
    print("\n📊 [2/3] Testing WEEKLY aggregations...")
    print("-" * 60)
    aggregator.create_weekly_kpis()
    
    # Check weekly results
    steam_weekly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM steam_weekly_kpis")
    twitch_weekly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM twitch_weekly_kpis")
    igdb_weekly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM igdb_ratings_weekly")
    
    print(f"✅ steam_weekly_kpis: {steam_weekly['count'][0]} records")
    print(f"✅ twitch_weekly_kpis: {twitch_weekly['count'][0]} records")
    print(f"✅ igdb_ratings_weekly: {igdb_weekly['count'][0]} records")
    
    # Test monthly aggregations
    print("\n📊 [3/3] Testing MONTHLY aggregations...")
    print("-" * 60)
    aggregator.create_monthly_kpis()
    
    # Check monthly results
    steam_monthly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM steam_monthly_kpis")
    twitch_monthly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM twitch_monthly_kpis")
    igdb_monthly = aggregator.db_manager.query("SELECT COUNT(*) as count FROM igdb_ratings_monthly")
    
    print(f"✅ steam_monthly_kpis: {steam_monthly['count'][0]} records")
    print(f"✅ twitch_monthly_kpis: {twitch_monthly['count'][0]} records")
    print(f"✅ igdb_ratings_monthly: {igdb_monthly['count'][0]} records")
    
    # Display sample data
    print("\n" + "=" * 60)
    print("📊 SAMPLE DATA")
    print("=" * 60)
    
    print("\n🎮 STEAM DAILY (top 3 by peak CCU):")
    steam_sample = aggregator.db_manager.query("""
        SELECT date, game_name, avg_ccu, peak_ccu, samples
        FROM steam_daily_kpis
        ORDER BY peak_ccu DESC
        LIMIT 3
    """)
    for _, row in steam_sample.iterrows():
        print(f"  {row['date']} | {row['game_name']}: Peak {row['peak_ccu']:,}, Avg {row['avg_ccu']:,.0f}, Samples: {row['samples']}")
    
    print("\n📺 TWITCH DAILY (top 3 by peak viewers):")
    twitch_sample = aggregator.db_manager.query("""
        SELECT date, game_name, avg_viewers, peak_viewers, samples
        FROM twitch_daily_kpis
        ORDER BY peak_viewers DESC
        LIMIT 3
    """)
    for _, row in twitch_sample.iterrows():
        print(f"  {row['date']} | {row['game_name']}: Peak {row['peak_viewers']:,}, Avg {row['avg_viewers']:,.0f}, Samples: {row['samples']}")

print("\n" + "=" * 60)
print("✨ AGGREGATION PIPELINE TEST COMPLETE!")
print("=" * 60)

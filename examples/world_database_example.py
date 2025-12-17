"""
Test with World database - contains countries, cities, and languages.
This demonstrates validation of geographic and demographic data.
"""

import sqlite3
import os
import urllib.request
from db_expectations import DatabaseValidator
from db_expectations.suites import ExpectationSuites

# Download World SQLite database
DB_URL = "https://raw.githubusercontent.com/sumitcfe/test_db/master/world.sqlite"
DB_PATH = "world.db"

print("="*70)
print("WORLD DATABASE VALIDATION TEST")
print("="*70)

# Download database if not exists
if not os.path.exists(DB_PATH):
    print(f"\nDownloading World database from GitHub...")
    try:
        urllib.request.urlretrieve(DB_URL, DB_PATH)
        print(f"✓ Downloaded: {DB_PATH}")
    except Exception as e:
        print(f"✗ Download failed: {e}")
        print("Creating sample database instead...")
        
        # Create a sample world database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE country (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                continent TEXT NOT NULL,
                region TEXT NOT NULL,
                surface_area REAL NOT NULL,
                independence_year INTEGER,
                population INTEGER NOT NULL,
                life_expectancy REAL,
                gnp REAL,
                capital INTEGER
            )
        """)
        
        cursor.execute("""
            CREATE TABLE city (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                country_code TEXT NOT NULL,
                district TEXT NOT NULL,
                population INTEGER NOT NULL,
                FOREIGN KEY (country_code) REFERENCES country(code)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE country_language (
                country_code TEXT NOT NULL,
                language TEXT NOT NULL,
                is_official TEXT NOT NULL,
                percentage REAL NOT NULL,
                PRIMARY KEY (country_code, language),
                FOREIGN KEY (country_code) REFERENCES country(code)
            )
        """)
        
        # Insert sample data
        countries = [
            ('USA', 'United States', 'North America', 'North America', 9372610, 1776, 331002651, 78.9, 21427700, 1),
            ('CHN', 'China', 'Asia', 'Eastern Asia', 9572900, -1523, 1439323776, 76.9, 14342903, 2),
            ('IND', 'India', 'Asia', 'Southern Asia', 3287263, 1947, 1380004385, 69.7, 2875142, 3),
            ('BRA', 'Brazil', 'South America', 'South America', 8515767, 1822, 212559417, 75.9, 1839758, 4),
            ('RUS', 'Russia', 'Europe', 'Eastern Europe', 17098242, 1991, 145934462, 72.6, 1699876, 5),
            ('JPN', 'Japan', 'Asia', 'Eastern Asia', 377930, 660, 126476461, 84.6, 5081770, 6),
            ('DEU', 'Germany', 'Europe', 'Western Europe', 357114, 1871, 83783942, 81.3, 3846414, 7),
            ('GBR', 'United Kingdom', 'Europe', 'British Islands', 242900, 1066, 67886011, 81.3, 2827113, 8),
            ('FRA', 'France', 'Europe', 'Western Europe', 643801, 843, 65273511, 82.7, 2715518, 9),
            ('ITA', 'Italy', 'Europe', 'Southern Europe', 301336, 1861, 60461826, 83.5, 2003576, 10),
        ]
        cursor.executemany("INSERT INTO country VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", countries)
        
        cities = [
            (1, 'Washington', 'USA', 'District of Columbia', 705749),
            (2, 'Beijing', 'CHN', 'Beijing', 21540000),
            (3, 'New Delhi', 'IND', 'Delhi', 32941000),
            (4, 'Brasília', 'BRA', 'Distrito Federal', 3015268),
            (5, 'Moscow', 'RUS', 'Moscow', 12537954),
            (6, 'Tokyo', 'JPN', 'Tokyo', 13960000),
            (7, 'Berlin', 'DEU', 'Berlin', 3769495),
            (8, 'London', 'GBR', 'England', 9002488),
            (9, 'Paris', 'FRA', 'Île-de-France', 2165423),
            (10, 'Rome', 'ITA', 'Lazio', 2873494),
            (11, 'New York', 'USA', 'New York', 8336817),
            (12, 'Los Angeles', 'USA', 'California', 3979576),
            (13, 'Mumbai', 'IND', 'Maharashtra', 20411000),
            (14, 'São Paulo', 'BRA', 'São Paulo', 12325232),
            (15, 'Shanghai', 'CHN', 'Shanghai', 27058000),
        ]
        cursor.executemany("INSERT INTO city VALUES (?, ?, ?, ?, ?)", cities)
        
        languages = [
            ('USA', 'English', 'T', 86.2),
            ('USA', 'Spanish', 'F', 13.4),
            ('CHN', 'Chinese', 'T', 91.5),
            ('IND', 'Hindi', 'T', 41.0),
            ('IND', 'English', 'T', 12.0),
            ('BRA', 'Portuguese', 'T', 97.5),
            ('RUS', 'Russian', 'T', 81.5),
            ('JPN', 'Japanese', 'T', 99.2),
            ('DEU', 'German', 'T', 95.0),
            ('GBR', 'English', 'T', 97.3),
            ('FRA', 'French', 'T', 93.6),
            ('ITA', 'Italian', 'T', 93.8),
        ]
        cursor.executemany("INSERT INTO country_language VALUES (?, ?, ?, ?)", languages)
        
        conn.commit()
        conn.close()
        print(f"✓ Created sample database: {DB_PATH}")
else:
    print(f"\n✓ Using existing database: {DB_PATH}")

# Connect and explore
connection_string = f"sqlite:///{os.path.abspath(DB_PATH)}"
validator = DatabaseValidator(connection_string)

print("\n" + "="*70)
print("DATABASE SCHEMA EXPLORATION")
print("="*70)

# Get table information
tables_df = validator.query_to_dataframe("""
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name
""")

print(f"\nFound {len(tables_df)} tables:")
for table in tables_df['name']:
    try:
        count_df = validator.query_to_dataframe(f"SELECT COUNT(*) as count FROM {table}")
        print(f"  • {table}: {count_df['count'].iloc[0]:,} rows")
    except Exception as e:
        print(f"  • {table}: (error)")

print("\n" + "="*70)
print("TEST 1: COUNTRY DATA VALIDATION")
print("="*70)

# Validate country data
country_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["code", "name", "continent", "population"]),
    ExpectationSuites.unique_checks(["code"]),
    ExpectationSuites.range_checks({
        "population": {"min": 0},
        "surface_area": {"min": 0}
    }),
    ExpectationSuites.set_membership_checks({
        "continent": ["Africa", "Antarctica", "Asia", "Europe", "North America", "Oceania", "South America"]
    }),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    country_results = validator.validate_query(
        query="SELECT * FROM country",
        asset_name="country_validation",
        suite_name="country_check",
        expectations=country_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if country_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {country_results['statistics']['success_percent']:.1f}%")
    print(f"Expectations: {country_results['statistics']['successful_expectations']}/{country_results['statistics']['evaluated_expectations']}")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Show top countries by population
top_countries = validator.query_to_dataframe("""
    SELECT name, continent, population, life_expectancy, gnp
    FROM country
    ORDER BY population DESC
    LIMIT 10
""")
print("\nTop 10 Countries by Population:")
print(top_countries.to_string(index=False))

# Continental statistics
continent_stats = validator.query_to_dataframe("""
    SELECT 
        continent,
        COUNT(*) as country_count,
        SUM(population) as total_population,
        ROUND(AVG(life_expectancy), 2) as avg_life_expectancy,
        ROUND(SUM(gnp), 2) as total_gnp
    FROM country
    GROUP BY continent
    ORDER BY total_population DESC
""")
print("\nStatistics by Continent:")
print(continent_stats.to_string(index=False))

print("\n" + "="*70)
print("TEST 2: CITY DATA VALIDATION")
print("="*70)

# Validate city data
city_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["id", "name", "country_code", "population"]),
    ExpectationSuites.unique_checks(["id"]),
    ExpectationSuites.range_checks({
        "population": {"min": 0}
    }),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    city_results = validator.validate_query(
        query="SELECT * FROM city",
        asset_name="city_validation",
        suite_name="city_check",
        expectations=city_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if city_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {city_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Top cities
top_cities = validator.query_to_dataframe("""
    SELECT 
        c.name as city_name,
        co.name as country_name,
        c.district,
        c.population
    FROM city c
    JOIN country co ON c.country_code = co.code
    ORDER BY c.population DESC
    LIMIT 15
""")
print("\nTop 15 Largest Cities:")
print(top_cities.to_string(index=False))

print("\n" + "="*70)
print("TEST 3: LANGUAGE DATA VALIDATION")
print("="*70)

# Validate language data
language_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["country_code", "language", "percentage"]),
    ExpectationSuites.range_checks({
        "percentage": {"min": 0, "max": 100}
    }),
    ExpectationSuites.set_membership_checks({
        "is_official": ["T", "F"]
    }),
    ExpectationSuites.row_count_check(min_rows=1)
)

try:
    language_results = validator.validate_query(
        query="SELECT * FROM country_language",
        asset_name="language_validation",
        suite_name="language_check",
        expectations=language_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if language_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {language_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

# Language distribution
language_dist = validator.query_to_dataframe("""
    SELECT 
        cl.language,
        COUNT(DISTINCT cl.country_code) as country_count,
        ROUND(AVG(cl.percentage), 2) as avg_percentage,
        COUNT(CASE WHEN cl.is_official = 'T' THEN 1 END) as official_in_countries
    FROM country_language cl
    GROUP BY cl.language
    ORDER BY country_count DESC
""")
print("\nLanguage Distribution:")
print(language_dist.to_string(index=False))

print("\n" + "="*70)
print("TEST 4: DEMOGRAPHIC ANALYSIS")
print("="*70)

# Analyze countries with high life expectancy and GDP
demo_query = """
    SELECT 
        name,
        continent,
        population,
        life_expectancy,
        gnp,
        ROUND(gnp * 1000000.0 / population, 2) as gdp_per_capita
    FROM country
    WHERE life_expectancy IS NOT NULL AND gnp IS NOT NULL
    ORDER BY life_expectancy DESC
    LIMIT 10
"""

demo_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["name", "life_expectancy"]),
    ExpectationSuites.range_checks({
        "life_expectancy": {"min": 0, "max": 100},
        "gdp_per_capita": {"min": 0}
    })
)

try:
    demo_results = validator.validate_query(
        query=demo_query,
        asset_name="demographic_analysis",
        suite_name="demo_check",
        expectations=demo_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if demo_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {demo_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

demo_df = validator.query_to_dataframe(demo_query)
print("\nCountries with Highest Life Expectancy:")
print(demo_df.to_string(index=False))

print("\n" + "="*70)
print("TEST 5: URBAN POPULATION ANALYSIS")
print("="*70)

# Analyze urbanization
urban_query = """
    SELECT 
        co.name as country_name,
        co.continent,
        co.population as country_population,
        COUNT(c.id) as city_count,
        SUM(c.population) as urban_population,
        ROUND(100.0 * SUM(c.population) / co.population, 2) as urbanization_rate
    FROM country co
    JOIN city c ON co.code = c.country_code
    GROUP BY co.code, co.name, co.continent, co.population
    ORDER BY urbanization_rate DESC
"""

urban_expectations = ExpectationSuites.combine(
    ExpectationSuites.null_checks(["country_name", "country_population"]),
    ExpectationSuites.range_checks({
        "city_count": {"min": 1},
        "urban_population": {"min": 0},
        "urbanization_rate": {"min": 0, "max": 100}
    })
)

try:
    urban_results = validator.validate_query(
        query=urban_query,
        asset_name="urban_analysis",
        suite_name="urban_check",
        expectations=urban_expectations
    )
    
    print(f"Validation: {'✓ PASSED' if urban_results['success'] else '✗ FAILED'}")
    print(f"Success Rate: {urban_results['statistics']['success_percent']:.1f}%")
except Exception as e:
    print(f"✗ Validation failed: {e}")

urban_df = validator.query_to_dataframe(urban_query)
print("\nUrbanization Rates by Country:")
print(urban_df.to_string(index=False))

print("\n" + "="*70)
print("SUMMARY")
print("="*70)

# Calculate overall results
all_tests = [
    ("Country Data", country_results),
    ("City Data", city_results),
    ("Language Data", language_results),
    ("Demographic Analysis", demo_results),
    ("Urban Analysis", urban_results)
]

passed = sum(1 for _, r in all_tests if r["success"])
total = len(all_tests)

print(f"\nTotal Tests: {total}")
print(f"Passed: {passed}")
print(f"Failed: {total - passed}")
print(f"Success Rate: {(passed/total)*100:.1f}%")

print("\nDetailed Results:")
for name, results in all_tests:
    status = "✓ PASSED" if results["success"] else "✗ FAILED"
    print(f"  {name}: {status} ({results['statistics']['success_percent']:.1f}%)")

# Global insights
print("\n" + "="*70)
print("GLOBAL INSIGHTS")
print("="*70)

insights = validator.query_to_dataframe("""
    SELECT 
        (SELECT COUNT(*) FROM country) as total_countries,
        (SELECT COUNT(*) FROM city) as total_cities,
        (SELECT COUNT(DISTINCT language) FROM country_language) as total_languages,
        (SELECT SUM(population) FROM country) as world_population,
        (SELECT ROUND(AVG(life_expectancy), 2) FROM country WHERE life_expectancy IS NOT NULL) as avg_life_expectancy,
        (SELECT ROUND(SUM(gnp), 2) FROM country WHERE gnp IS NOT NULL) as world_gnp
""")

print(insights.to_string(index=False))

validator.close()
print("\n✓ Validation complete!")
print(f"✓ Database: {DB_PATH}")

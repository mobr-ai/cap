from datetime import datetime, timezone
from cap.util.sparql_date_processor import SparqlDateProcessor

# Test suite
if __name__ == "__main__":
    # Set a fixed reference time for consistent testing
    test_time = datetime.now(timezone.utc)
    processor = SparqlDateProcessor(reference_time=test_time)

    print("="*80)
    print("SPARQL Date Arithmetic Preprocessor - Test Suite")
    print("="*80)

    # Test case 1: Simple subtraction
    print("\n1. Test: Subtract 7 days")
    print("-" * 80)
    query1 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?oneWeekAgo
        WHERE {
            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
        }
    '''
    result1, count1 = processor.process(query1)
    print("Original:")
    print(query1)
    print("\nProcessed:")
    print(result1)
    print(f"\nReplacements: {count1}")

    # Test case 2: Add hours
    print("\n2. Test: Add 24 hours")
    print("-" * 80)
    query2 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?tomorrow
        WHERE {
            BIND (NOW() + "PT24H"^^xsd:duration as ?tomorrow)
        }
    '''
    result2, count2 = processor.process(query2)
    print("Original:")
    print(query2)
    print("\nProcessed:")
    print(result2)
    print(f"\nReplacements: {count2}")

    # Test case 3: Complex duration
    print("\n3. Test: Complex duration (1 day, 12 hours, 30 minutes)")
    print("-" * 80)
    query3 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?pastTime
        WHERE {
            BIND (NOW() - "P1DT12H30M"^^xsd:dayTimeDuration as ?pastTime)
        }
    '''
    result3, count3 = processor.process(query3)
    print("Original:")
    print(query3)
    print("\nProcessed:")
    print(result3)
    print(f"\nReplacements: {count3}")

    # Test case 4: Multiple BIND statements
    print("\n4. Test: Multiple BIND statements in one query")
    print("-" * 80)
    query4 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?pastWeek ?pastDay ?futureWeek
        WHERE {
            BIND((NOW() - "P7D"^^xsd:dayTimeDuration) AS ?oneWeekAgo)
            BIND(NOW() - "P7D"^^xsd:dayTimeDuration as ?lastWeek)
            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?pastWeek)
            BIND (NOW() - "P1D"^^xsd:dayTimeDuration as ?pastDay)
            BIND (NOW() + "P7D"^^xsd:duration as ?futureWeek)
            FILTER(?timestamp >= NOW() - "P7D"^^xsd:dayTimeDuration)
        }
    '''
    result4, count4 = processor.process(query4)
    print("Original:")
    print(query4)
    print("\nProcessed:")
    print(result4)
    print(f"\nReplacements: {count4}")

    # Test case 5: Chained operations (dateTime literal + duration)
    print("\n5. Test: Adding duration to a dateTime literal")
    print("-" * 80)
    query5 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?future
        WHERE {
            BIND ("2025-01-01T00:00:00Z"^^xsd:dateTime + "P30D"^^xsd:duration as ?future)
            FILTER(?timestamp >= NOW() - "P30D"^^xsd:dayTimeDuration)
        }
    '''
    result5, count5 = processor.process(query5)
    print("Original:")
    print(query5)
    print("\nProcessed:")
    print(result5)
    print(f"\nReplacements: {count5}")

    # Test case 6: Mixed case and whitespace
    print("\n6. Test: Mixed case and varied whitespace")
    print("-" * 80)
    query6 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?test
        WHERE {
            bind (  NOW(  )  -  "P7D"^^xsd:dayTimeDuration  AS  ?test  )
            BIND(NOW() - "P6M"^^xsd:dayTimeDuration AS ?startDate)
        }
    '''
    result6, count6 = processor.process(query6)
    print("Original:")
    print(query6)
    print("\nProcessed:")
    print(result6)
    print(f"\nReplacements: {count6}")

    # Test case 7: Query with no date arithmetic
    print("\n7. Test: Query with no date arithmetic (should not change)")
    print("-" * 80)
    query7 = '''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        SELECT ?value
        WHERE {
            BIND ("some value" as ?value)
        }
    '''
    result7, count7 = processor.process(query7)
    print("Original:")
    print(query7)
    print("\nProcessed:")
    print(result7)
    print(f"\nReplacements: {count7}")
    print(f"Unchanged: {query7 == result7}")

    print("\n" + "="*80)
    print("All tests completed!")
    print("="*80)

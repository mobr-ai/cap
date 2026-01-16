from datetime import datetime, timezone

from cap.util.sparql_util import force_limit_cap
from cap.util.sparql_date_processor import SparqlDateProcessor

# Test suite
if __name__ == "__main__":
    # Set a fixed reference time for consistent testing
    test_time = datetime.now(timezone.utc)
    processor = SparqlDateProcessor(reference_time=test_time)

    print("="*80)
    print("SPARQL Date Arithmetic Preprocessor - Test Suite")
    print("="*80)

    # Define test cases
    test_cases = [
        {
            "name": "Subtract 7 days",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?oneWeekAgo
                WHERE {
                    BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?oneWeekAgo)
                }
            '''
        },
        {
            "name": "Add 24 hours",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                SELECT ?tomorrow
                WHERE {
                    BIND (NOW() + "PT24H"^^xsd:duration as ?tomorrow)
                }
                LIMIT 10000
            '''
        },
        {
            "name": "Complex duration (1 day, 12 hours, 30 minutes)",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?pastTime
                WHERE {
                    BIND (NOW() - "P1DT12H30M"^^xsd:dayTimeDuration as ?pastTime)
                }
                LIMIT 10
            '''
        },
        {
            "name": "Multiple BIND statements in one query",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?pastWeek ?pastDay ?futureWeek
                WHERE {
                    {
                        SELECT ?x
                        WHERE {
                            ?x ?p ?o
                            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?innerPastWeek)
                        }
                        LIMIT 10000
                    }
                    BIND((NOW() - "P7D"^^xsd:dayTimeDuration) AS ?oneWeekAgo)
                    BIND(NOW() - "P7D"^^xsd:dayTimeDuration as ?lastWeek)
                    BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?pastWeek)
                    BIND (NOW() - "P1D"^^xsd:dayTimeDuration as ?pastDay)
                    BIND (NOW() + "P7D"^^xsd:duration as ?futureWeek)
                    FILTER(?timestamp >= NOW() - "P7D"^^xsd:dayTimeDuration)
                }
            '''
        },
        {
            "name": "Adding duration to a dateTime literal",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?future
                WHERE {
                    BIND ("2025-01-01T00:00:00Z"^^xsd:dateTime + "P30D"^^xsd:duration as ?future)
                    FILTER(?timestamp >= NOW() - "P30D"^^xsd:dayTimeDuration)
                }
            '''
        },
        {
            "name": "Mixed case and varied whitespace",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?test
                WHERE {
                    {
                        SELECT ?x
                        WHERE {
                            ?x ?p ?o
                            BIND (NOW() - "P7D"^^xsd:dayTimeDuration as ?innerPastWeek)
                        }
                        LIMIT 10000
                    }
                    bind (  NOW(  )  -  "P7D"^^xsd:dayTimeDuration  AS  ?test  )
                    BIND(NOW() - "P6M"^^xsd:dayTimeDuration AS ?startDate)
                }
                LIMIT 1
            '''
        },
        {
            "name": "Query with no date arithmetic (should only add limit)",
            "query": '''
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                SELECT ?value
                WHERE {
                    BIND ("some value" as ?value)
                }
            '''
        }
    ]

    # Run tests
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. Test: {test_case['name']}")
        print("-" * 80)

        original = test_case['query']
        query = force_limit_cap(original)
        result, count = processor.process(query)

        print("Original:")
        print(original)
        print("\nProcessed:")
        print(result)
        print(f"\nReplacements: {count}")

    print("\n" + "="*80)
    print("All tests completed!")
    print("="*80)

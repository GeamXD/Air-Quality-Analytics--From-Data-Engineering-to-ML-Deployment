from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

import re

# List of major Nigerian cities
major_cities = [
    "Lagos", "Abuja", "Kano", "Ibadan", "Port Harcourt", "Benin City",
    "Maiduguri", "Zaria", "Abeokuta", "Ilorin", "Calabar", "Ogun", "Enugu",
    "Kaduna", "Jos", "Onitsha"
]

@udf(returnType=StringType())
def extract_city(city_name):
    """
    This function checks if a city name from a dataset contains any major Nigerian city.
    If a major city is found, it returns that city.
    If no city is found, it defaults to 'Lagos'.

    Example usage:

    from pyspark import pipelines as dp
    from pyspark.sql.functions import col
    from utilities import new_utils

    @dp.table
    def my_table():
        return (
            spark.read.table("samples.locations")
            .withColumn("cleaned_city", new_utils.extract_city(col("City")))
        )
    """
    if city_name is None:
        return "Lagos"

    city_name_lower = city_name.lower()
    for city in major_cities:
        if city.lower() in city_name_lower:
            return city

    # Default city if no match
    return "Lagos"
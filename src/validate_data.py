import logging

def validate_transformed_data(metal_price_data, currency_rates_data):
    try:
        assert "date" in metal_price_data
        assert "metal_id" in metal_price_data
        assert "unit" in metal_price_data
        assert "price_usd" in metal_price_data
        
        for record in currency_rates_data:
            assert "currency_code" in record
            assert "date" in record
            assert "rate_to_usd" in record
        
        logging.info("Transformed data validation passed.")
    except AssertionError as e:
        logging.error(f"Transformed data validation failed: {e}")
        raise
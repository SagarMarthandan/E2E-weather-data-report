"""
This module provides functionality to fetch current weather data from the Weatherstack API.
"""
import requests

# API key for Weatherstack. Note: In a production environment, this should be stored securely.
api_key = "72cd6bcecb534f7f2b3e8c0a04c3b989"
# The endpoint URL for fetching current weather data for New York.
api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"

def fetch_data():
    """
    Fetches current weather data from the Weatherstack API.

    Returns:
        dict: A dictionary containing the JSON response from the API.

    Raises:
        requests.RequestException: If there is an issue with the network request.
    """
    print("Fetching data from weatherstack API.....")
    
    try:
        # Send a GET request to the Weatherstack API
        response = requests.get(api_url)
        # Raise an exception for HTTP errors (4xx or 5xx)
        response.raise_for_status()
        print("API response received successfully!")
        # Return the parsed JSON data
        return response.json()
    
    except requests.RequestException as e:
        # Log the error and re-raise the exception
        print(f"An error occurred: {e}")
        raise
import pandas as pd
import anthropic
import csv
import time
import os
from typing import Dict, List, Any, Optional

# Initialize the Claude client
# You'll need to replace this with your actual API key
client = anthropic.Anthropic(
    api_key="YOUR_CLAUDE_API_KEY",  # Replace with your actual API key
)

# Function to read company data from CSV or Excel
def read_company_data(file_path: str) -> pd.DataFrame:
    """Read company data from the provided file path."""
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")

# Function to analyze a company using Claude
def analyze_company(company_name: str, phone_number: str, city: str, state: str) -> Dict[str, Any]:
    """
    Send company information to Claude and get analysis results.
    """
    prompt = f"""
    I need you to analyze this company:
    
    Company Name: {company_name}
    Phone Number: {phone_number}
    City: {city}
    State: {state}
    
    Please search for information about this company and provide the following:
    1. Do they have a website? If yes, provide the URL.
    2. What type of website do they have (professional, outdated, mobile-friendly, etc.)?
    3. Could they benefit from a professional website remake? Why?
    4. Find any social media profiles for this company.
    5. Find any business directories they're listed in.
    6. What type of business are they?
    7. Based on your analysis, what services would benefit them the most?
    8. Write a sales script tailored to their specific needs based on your findings.
    
    Format your response in a JSON structure with the following keys:
    - company_name
    - has_website (true/false)
    - website_url (if any)
    - website_notes
    - needs_website_remake (true/false)
    - business_type
    - social_media_links (as an array)
    - directory_listings (as an array)
    - recommended_services (as an array)
    - sales_script
    
    Only return the JSON with no other text.
    """
    
    try:
        response = client.messages.create(
            model="claude-3-opus-20240229",  # Or your preferred Claude model
            max_tokens=4000,
            temperature=0.0,
            system="You are a marketing consultant tasked with analyzing businesses. You will be provided company information and must return a structured JSON analysis with recommendations and a sales script.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Extract JSON response
        from json import loads, JSONDecodeError
        try:
            result = loads(response.content[0].text)
            return result
        except JSONDecodeError:
            # If Claude doesn't return proper JSON, try to extract usable information
            return {
                "company_name": company_name,
                "has_website": False,
                "website_url": "",
                "website_notes": "Error analyzing company",
                "needs_website_remake": False,
                "business_type": "",
                "social_media_links": [],
                "directory_listings": [],
                "recommended_services": [],
                "sales_script": "Error generating sales script"
            }
            
    except Exception as e:
        print(f"Error analyzing {company_name}: {str(e)}")
        return {
            "company_name": company_name,
            "error": str(e)
        }

# Main function to process all companies and save results
def analyze_companies(input_file_path: str, output_file_path: str) -> None:
    """
    Process all companies in the input file and save results to a CSV file.
    """
    # Read company data
    df = read_company_data(input_file_path)
    
    # Check if required columns exist
    required_columns = ['company_name', 'phone_number', 'city', 'state']
    for col in required_columns:
        if col not in df.columns:
            print(f"Error: Column '{col}' not found in the input file.")
            return
    
    # Initialize results list
    results = []
    
    # Process each company
    for idx, row in df.iterrows():
        print(f"Processing {idx+1}/{len(df)}: {row['company_name']}")
        
        # Analyze company
        analysis = analyze_company(
            company_name=row['company_name'],
            phone_number=str(row['phone_number']),
            city=row['city'],
            state=row['state']
        )
        
        # Add to results
        results.append(analysis)
        
        # Wait a bit to avoid rate limiting
        time.sleep(1)
    
    # Convert to DataFrame for easier CSV writing
    results_df = pd.DataFrame(results)
    
    # Save to CSV
    results_df.to_csv(output_file_path, index=False)
    print(f"Analysis complete! Results saved to {output_file_path}")

# Example usage in your Jupyter notebook:
# Replace with your actual file paths
input_file = "your_company_list.csv"  # Update this
output_file = "company_analysis_results.csv"  # Update this if needed

# Run the analysis
analyze_companies(input_file, output_file)
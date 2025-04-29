import pandas as pd
import anthropic
import csv
import time
import os
import json
import re
from typing import Dict, List, Any, Optional

def process_companies(input_file: str, output_file: str, api_key: str, start_index: int = 0, count: Optional[int] = None, delay: float = 1.0) -> str:
    """
    Process a batch of companies and save the results to a CSV file.
    
    Args:
        input_file: Path to the input CSV file with company data
        output_file: Path where results will be saved
        api_key: Anthropic API key
        start_index: Index of the first company to process (0-based)
        count: Number of companies to process (None for all)
        delay: Delay between API calls in seconds
        
    Returns:
        Path to the output file
    """
    # Initialize the Claude client
    client = anthropic.Anthropic(api_key=api_key)
    
    # Read company data
    df = pd.read_csv(input_file)
    
    # Check for required columns
    required_cols = ['name', 'phone_number', 'city', 'state']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {', '.join(missing_cols)}")
    
    # Select batch to process
    start = start_index
    end = len(df) if count is None else start_index + count
    companies = df.iloc[start:end].to_dict('records')
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    
    # Process companies
    results = []
    for i, company in enumerate(companies):
        print(f"Processing {i+1}/{len(companies)}: {company['name']} in {company['city']}, {company['state']}")
        result = analyze_company(client, company)
        results.append(result)
        
        # Save progress after each company in case of interruption
        temp_df = pd.DataFrame(results)
        temp_df.to_csv(output_file, index=False)
        
        # Delay to avoid rate limiting
        if i < len(companies) - 1:  # Don't delay after the last item
            time.sleep(delay)
    
    print(f"✅ Completed! Processed {len(results)} companies. Results saved to {output_file}")
    return output_file

def extract_json_from_text(text):
    """Extract valid JSON from text that may contain Markdown code blocks."""
    # First check if the text starts with a code block marker
    if text.strip().startswith("```json"):
        # Extract content between ```json and ``` markers
        pattern = r"```json\s*([\s\S]*?)\s*```"
        match = re.search(pattern, text, re.DOTALL)
        if match:
            return match.group(1).strip()
    
    # If no code block markers or extraction failed, return the original text
    return text.strip()

def analyze_company(client, company: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze a single company using Claude API.
    
    Args:
        client: Anthropic client
        company: Dictionary with company information
        
    Returns:
        Analysis results as a dictionary
    """
    # Extract company information
    name = company['name']
    phone = str(company['phone_number'])
    city = company['city']
    state = company['state']
    
    # Create prompt for Claude
    prompt = f"""
    I need you to analyze this company:
    
    Company Name: {name}
    Phone Number: {phone}
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
    
    Only return the JSON with no other text or markdown formatting.
    """
    
    try:
        # Call Claude API
        response = client.messages.create(
            model="claude-3-7-sonnet-20250219",
            max_tokens=4000,
            temperature=0.0,
            system="You are a marketing consultant tasked with analyzing businesses. You will be provided company information and must return a structured JSON analysis with recommendations and a sales script. Return ONLY the JSON with no markdown code block formatting.",
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        # Get the raw response text
        raw_response = response.content[0].text
        
        # Extract JSON from the response
        json_content = extract_json_from_text(raw_response)
        
        try:
            # Parse the JSON
            result = json.loads(json_content)
            print(f"✓ Successfully parsed JSON for {name}")
            return result
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON parse error for {name}: {str(e)}")
            print(f"Cleaned content: {json_content[:100]}...")
            
            # Try one more time with even more aggressive cleaning
            try:
                # Remove any remaining non-JSON characters
                cleaner_content = re.sub(r"```.*?```", "", raw_response, flags=re.DOTALL)
                cleaner_content = cleaner_content.strip()
                result = json.loads(cleaner_content)
                print(f"✓ Parsing succeeded with aggressive cleaning for {name}")
                return result
            except:
                print(f"❌ All parsing attempts failed for {name}")
                
                # Return error information
                return {
                    "company_name": name,
                    "has_website": False,
                    "website_url": "",
                    "website_notes": "Error parsing response",
                    "needs_website_remake": False,
                    "business_type": "",
                    "social_media_links": [],
                    "directory_listings": [],
                    "recommended_services": [],
                    "sales_script": "Error parsing response",
                    "_error": str(e),
                    "_raw_response": raw_response[:500]  # Store first 500 chars for debugging
                }
            
    except Exception as e:
        print(f"❌ API error for {name}: {str(e)}")
        return {
            "company_name": name,
            "error": str(e)
        }

# Example usage
if __name__ == "__main__":
    api_key = ""
    
    output_path = process_companies(
        input_file="drizzle-data.csv",
        output_file="results/claude_analysis.csv",
        api_key=api_key,
        start_index=0,
        count=5,
        delay=1.5
    )
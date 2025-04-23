from mcp.server.fastmcp import FastMCP
import os
from dotenv import load_dotenv
import logging
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
import json
from google.auth.exceptions import GoogleAuthError
from gspread.exceptions import APIError, SpreadsheetNotFound
from typing import List, Dict, Any

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Create an MCP server
mcp = FastMCP("GoogleSheetsServer")

class GoogleSheetsClient:
    def __init__(self):
        # Get the directory of the current script
        current_dir = Path(__file__).parent.absolute()
        credentials_path = current_dir / 'credentials.json'
        
        if not credentials_path.exists():
            raise FileNotFoundError(
                f"credentials.json file not found at {credentials_path}. "
                "Please make sure the file exists in the same directory as main.py"
            )
        
        try:
            # Read and log the service account email
            with open(credentials_path) as f:
                creds_data = json.load(f)
                service_account_email = creds_data.get('client_email')
                logger.info(f"Using service account: {service_account_email}")
                logger.info(f"Project ID: {creds_data.get('project_id')}")
        except Exception as e:
            logger.error(f"Error reading credentials file: {str(e)}")
            raise
        
        # Set up the scope
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        
        try:
            # Create credentials using the service account
            self.creds = Credentials.from_service_account_file(
                str(credentials_path),
                scopes=scopes
            )
            
            # Authorize the client
            self.client = gspread.authorize(self.creds)
            logger.info("Successfully authenticated with Google Sheets API")
            
            # Test the connection by listing spreadsheets
            try:
                spreadsheets = self.client.openall()
                logger.info(f"Found {len(spreadsheets)} spreadsheets shared with the service account")
                if spreadsheets:
                    for sheet in spreadsheets:
                        logger.info(f"Spreadsheet: {sheet.title} (ID: {sheet.id})")
                else:
                    logger.warning("No spreadsheets found. Please make sure you've shared at least one spreadsheet with the service account.")
            except APIError as e:
                logger.error(f"API Error while listing spreadsheets: {str(e)}")
                logger.error("This might indicate a permissions issue or API quota exceeded")
            
        except GoogleAuthError as e:
            logger.error(f"Google Authentication Error: {str(e)}")
            logger.error("Please check if the service account credentials are valid")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during authentication: {str(e)}")
            raise

    def get_all_spreadsheets(self) -> list:
        """Get all spreadsheets shared with the service account"""
        try:
            # Get all spreadsheets
            spreadsheets = self.client.openall()
            
            if not spreadsheets:
                logger.warning("No spreadsheets found. Make sure you've shared spreadsheets with the service account email.")
                return []
            
            # Format the response
            result = []
            for spreadsheet in spreadsheets:
                sheet_info = {
                    "title": spreadsheet.title,
                    "id": spreadsheet.id,
                    "url": spreadsheet.url
                }
                logger.info(f"Found spreadsheet: {sheet_info['title']} (ID: {sheet_info['id']})")
                result.append(sheet_info)
            
            return result
        except APIError as e:
            logger.error(f"API Error while getting spreadsheets: {str(e)}")
            logger.error("This might indicate a permissions issue or API quota exceeded")
            return [{"error": f"API Error: {str(e)}"}]
        except Exception as e:
            logger.error(f"Error getting spreadsheets: {str(e)}")
            return [{"error": str(e)}]

    def get_spreadsheet_info(self, spreadsheet_id: str) -> dict:
        """Get information about a specific spreadsheet"""
        try:
            logger.info(f"Attempting to open spreadsheet with ID: {spreadsheet_id}")
            # Open the spreadsheet by ID
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Get all worksheets
            worksheets = spreadsheet.worksheets()
            logger.info(f"Found {len(worksheets)} worksheets in {spreadsheet.title}")
            
            # Format the response
            result = {
                "title": spreadsheet.title,
                "id": spreadsheet.id,
                "url": spreadsheet.url,
                "sheets": []
            }
            
            for worksheet in worksheets:
                sheet_info = {
                    "title": worksheet.title,
                    "row_count": worksheet.row_count,
                    "col_count": worksheet.col_count
                }
                logger.info(f"Found worksheet: {sheet_info['title']}")
                result["sheets"].append(sheet_info)
            
            return result
        except SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID {spreadsheet_id} not found")
            logger.error("Please check if the spreadsheet ID is correct and if it's shared with the service account")
            return {"error": f"Spreadsheet not found. Please check the ID and sharing permissions."}
        except APIError as e:
            logger.error(f"API Error while accessing spreadsheet: {str(e)}")
            logger.error("This might indicate a permissions issue or API quota exceeded")
            return {"error": f"API Error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error getting spreadsheet info: {str(e)}")
            return {"error": str(e)}

    def get_sheet_data(self, spreadsheet_id: str, sheet_title: str) -> dict:
        """Get data from a specific sheet"""
        try:
            logger.info(f"Attempting to get data from {sheet_title} in spreadsheet {spreadsheet_id}")
            # Open the spreadsheet by ID and worksheet by title
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_title)
            
            # Get all values
            values = worksheet.get_all_values()
            logger.info(f"Retrieved {len(values)} rows from {sheet_title}")
            
            # Format the response
            return {
                "title": worksheet.title,
                "data": values
            }
        except SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID {spreadsheet_id} not found")
            logger.error("Please check if the spreadsheet ID is correct and if it's shared with the service account")
            return {"error": f"Spreadsheet not found. Please check the ID and sharing permissions."}
        except APIError as e:
            logger.error(f"API Error while accessing sheet: {str(e)}")
            logger.error("This might indicate a permissions issue or API quota exceeded")
            return {"error": f"API Error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error getting sheet data: {str(e)}")
            return {"error": str(e)}

    def format_sheet_data(self, data: dict) -> str:
        """Format sheet data into a readable text format"""
        if "error" in data:
            return f"Error: {data['error']}"
        
        if "data" not in data or not data["data"]:
            return "No data found in the sheet"
        
        rows = data["data"]
        if not rows:
            return "Sheet is empty"
        
        # Format header row
        header = " | ".join(rows[0])
        separator = "-" * len(header)
        
        # Format data rows
        formatted_rows = [header, separator]
        for row in rows[1:]:
            formatted_rows.append(" | ".join(str(cell) for cell in row))
        
        return "\n".join(formatted_rows)

    def get_sheet_attributes(self, spreadsheet_id: str, sheet_title: str) -> List[str]:
        """Get the column headers (attributes) from a sheet"""
        try:
            logger.info(f"Getting attributes from {sheet_title} in spreadsheet {spreadsheet_id}")
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_title)
            
            # Get the first row (headers)
            headers = worksheet.row_values(1)
            logger.info(f"Found attributes: {headers}")
            return headers
        except Exception as e:
            logger.error(f"Error getting sheet attributes: {str(e)}")
            return []

    def add_data_to_sheet(self, spreadsheet_id: str, sheet_title: str, data: List[List[Any]]) -> bool:
        """Add data to a sheet"""
        try:
            logger.info(f"Adding {len(data)} records to {sheet_title} in spreadsheet {spreadsheet_id}")
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_title)
            
            # Get the next empty row
            next_row = len(worksheet.get_all_values()) + 1
            
            # Update the sheet with new data
            worksheet.update(f'A{next_row}', data)
            logger.info("Data added successfully")
            return True
        except Exception as e:
            logger.error(f"Error adding data to sheet: {str(e)}")
            return False

@mcp.tool()
def list_spreadsheets() -> list:
    """List all spreadsheets shared with the service account"""
    try:
        client = GoogleSheetsClient()
        return client.get_all_spreadsheets()
    except Exception as e:
        return [{"error": str(e)}]

@mcp.tool()
def get_spreadsheet_info(spreadsheet_id: str) -> dict:
    """Get information about a specific spreadsheet"""
    try:
        client = GoogleSheetsClient()
        return client.get_spreadsheet_info(spreadsheet_id)
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def get_sheet_content(spreadsheet_id: str, sheet_title: str) -> str:
    """Get the content of a specific sheet"""
    try:
        client = GoogleSheetsClient()
        data = client.get_sheet_data(spreadsheet_id, sheet_title)
        return client.format_sheet_data(data)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
def generate_sheet_data(spreadsheet_id: str, sheet_title: str, num_records: int) -> str:
    """Generate data for a sheet based on its attributes"""
    try:
        client = GoogleSheetsClient()
        
        # Get the sheet attributes
        attributes = client.get_sheet_attributes(spreadsheet_id, sheet_title)
        if not attributes:
            return "Error: Could not get sheet attributes. Please check if the sheet exists and has headers."
        
        # Format the attributes for the LLM prompt
        attributes_str = ", ".join(attributes)
        
        # Create a prompt for the LLM to generate data
        prompt = f"""
        I need you to generate {num_records} realistic records for a Google Sheet with the following columns: {attributes_str}
        
        The data should be returned in a specific format. Here's an example of how the data should look:
        
        [
            ["John Doe", "john.doe@example.com", "01/15/2023", 99.99, 5, "Active", "Product description"],
            ["Jane Smith", "jane.smith@example.com", "02/20/2023", 149.99, 3, "Pending", "Another product"]
        ]
        
        Important:
        1. Return ONLY the data array, nothing else
        2. Each record must have exactly {len(attributes)} values
        3. Values should match the data type of each column
        4. The data should be realistic and consistent
        5. Do not include any explanations or additional text
        """
        
        # The LLM will generate the data based on the prompt
        # The data will be in the format: [[value1, value2, ...], [value1, value2, ...], ...]
        data = []  # This will be populated by the LLM
        
        # Validate the generated data
        if not data:
            return "Error: No data was generated by the LLM"
            
        if not isinstance(data, list):
            return "Error: Generated data is not in the correct format"
            
        if len(data) != num_records:
            return f"Error: Expected {num_records} records but got {len(data)}"
            
        for record in data:
            if len(record) != len(attributes):
                return f"Error: Record has {len(record)} values but expected {len(attributes)}"
        
        # Return the generated data as a string
        return str(data)
    except Exception as e:
        logger.error(f"Error in generate_sheet_data: {str(e)}")
        return f"Error: {str(e)}"

@mcp.tool()
def add_data_to_sheet(spreadsheet_id: str, sheet_title: str, data_str: str) -> str:
    """Add pre-generated data to a sheet"""
    try:
        client = GoogleSheetsClient()
        
        # Convert the string data back to a list
        try:
            # First try to parse as a list of lists
            data = eval(data_str)
            if not isinstance(data, list):
                # If not a list, try to parse as CSV
                data = [line.split(',') for line in data_str.strip().split('\n')]
        except:
            # If both attempts fail, try to parse as CSV
            try:
                data = [line.split(',') for line in data_str.strip().split('\n')]
            except:
                return "Error: Invalid data format. Please provide data in the correct format (either as a list of lists or as CSV)."
        
        # Add data to sheet
        success = client.add_data_to_sheet(spreadsheet_id, sheet_title, data)
        if success:
            return f"Successfully added {len(data)} records to {sheet_title}"
        else:
            return "Error: Failed to add data to the sheet"
    except Exception as e:
        logger.error(f"Error in add_data_to_sheet: {str(e)}")
        return f"Error: {str(e)}"

@mcp.tool()
def update_sheet_record(spreadsheet_id: str, sheet_title: str, identifier: str, updates: str) -> str:
    """Update a record in the sheet based on id or name matching
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
        sheet_title: The title of the sheet
        identifier: The id or name to match for updating
        updates: String containing updates in format "column1=value1 column2=value2"
                Example: "age=25 email=xyz@example.com phone=1234567890"
    """
    try:
        client = GoogleSheetsClient()
        
        # Get the sheet data
        spreadsheet = client.client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_title)
        
        # Get all values
        all_values = worksheet.get_all_values()
        if not all_values:
            return "Error: Sheet is empty"
            
        # Get headers
        headers = all_values[0]
        
        # Determine identifier column (id or name)
        identifier_col = None
        if 'id' in headers:
            identifier_col = headers.index('id')
        elif 'name' in headers:
            identifier_col = headers.index('name')
        else:
            return "Error: Sheet must have either 'id' or 'name' column as identifier"
        
        # Parse updates into a dictionary
        update_dict = {}
        for update in updates.split():
            if '=' in update:
                col, val = update.split('=', 1)
                if col in headers:
                    update_dict[col] = val
                else:
                    return f"Error: Column '{col}' not found in sheet"
        
        if not update_dict:
            return "Error: No valid updates provided"
        
        # Find the row with matching identifier
        row_index = None
        for i, row in enumerate(all_values[1:], start=2):  # Skip header row
            if row[identifier_col].lower() == identifier.lower():
                row_index = i
                break
                
        if row_index is None:
            identifier_type = 'id' if 'id' in headers else 'name'
            return f"Error: No record found with {identifier_type} '{identifier}'"
        
        # Prepare updates
        updates = []
        for col, val in update_dict.items():
            col_index = headers.index(col)
            updates.append((row_index, col_index + 1, val))  # +1 because gspread is 1-based
        
        # Apply updates
        for row, col, val in updates:
            worksheet.update_cell(row, col, val)
            
        identifier_type = 'id' if 'id' in headers else 'name'
        return f"Successfully updated record with {identifier_type} '{identifier}' with: {', '.join(f'{k}={v}' for k, v in update_dict.items())}"
        
    except Exception as e:
        logger.error(f"Error in update_sheet_record: {str(e)}")
        return f"Error: {str(e)}"

@mcp.prompt()
def sheets_prompt() -> str:
    """Create a prompt template for Google Sheets interaction"""
    return """
    I'll help you manage data in your Google Sheets.
    
    To generate data for a sheet, use:
    /tool generate_sheet_data "spreadsheet_id" "sheet_title" number_of_records
    
    To add the generated data to a sheet, use:
    /tool add_data_to_sheet "spreadsheet_id" "sheet_title" "data_string"
    
    To update a record in the sheet, use:
    /tool update_sheet_record "spreadsheet_id" "sheet_title" "identifier" "updates"
    
    The data string can be in one of these formats:
    1. List of lists format: [["value1", "value2"], ["value3", "value4"]]
    2. CSV format: value1,value2
                  value3,value4
    
    For updates, provide the changes in format: column1=value1 column2=value2
    Example: age=25 email=xyz@example.com phone=1234567890
    
    Note: 
    1. The identifier can be either an 'id' or 'name' value, depending on which column exists in your sheet
    2. The sheet must have either an 'id' or 'name' column to identify records
    3. Make sure the credentials.json file is in the same directory as main.py
    4. The spreadsheet must be shared with the service account email address
    5. The service account must have the necessary permissions
    6. The spreadsheet ID can be found in the URL of your Google Sheet (the long string between /d/ and /edit)
    
    If you encounter authentication issues:
    1. Check if the service account email has been added as an editor to the spreadsheet
    2. Verify that the credentials.json file is valid and not corrupted
    3. Ensure the spreadsheet ID is correct
    4. Check if the Google Sheets API is enabled in your Google Cloud project
    """




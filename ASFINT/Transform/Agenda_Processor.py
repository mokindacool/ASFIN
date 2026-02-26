import numpy as np
import pandas as pd
import re
from datetime import datetime

from ASFINT.Utility.Cleaning import in_df, is_type
from ASFINT.Utility.Utils import column_converter

def _find_chunk_pattern(starts, ends, end_prepattern = '\d+\s'):
      """
      Extracts a chunk of text from 'inpt' text based on start and end keywords.
      starts (list[str]): List of keywords to start the chunk of text we want to extract
      ends (list[str]): List of keywords to end the chunk of text we want to extract
      end_prepattern (str): Regex pattern to append before every end keyword
         - for example meeting agendas are structure 1 Contingency, 2 Sponsorship (periods removed during preprocessing), so if you want the chunk to end at 'Sponsorship' you add in a ending prepattern to catch the '2 '.

      example pattern to construct: Contingency\s*([\s\S]*)(?:\d+\sAdjournment|\d+\sSponsorship)
      
      """
      assert len(starts) != 0, 'starts is an empty list'
      assert is_type(starts, str), 'starts is not a list of strings'
      assert len(ends) != 0, 'ends is an empty list'
      assert is_type(ends, str), 'ends is not a list of strings'

      assert isinstance(end_prepattern, str), f'end_prepattern should be a string but is type {type(end_prepattern)}'
      
      pattern = ''
      if len(starts) == 1:
          pattern += starts[0]
      else: 
         pattern = '(?:'
         for start_keyword in starts[:-1]: 
            pattern += start_keyword + '|'
         pattern += starts[-1] + ')'
      
      pattern += '\s*?([\s\S]*?)(?:' # make sure to have the '*?' to do non-greedy matching

      if len(ends) == 1:
         pattern += ends[0]
      else: 
         for end_keyword in ends[:-1]: 
            pattern += end_prepattern + end_keyword + '|'
         pattern += end_prepattern + ends[-1]

      pattern += ')'
      return pattern

# def _find_chunk_pattern(starts, ends, end_prepattern=r'\d\.\s'):
#     """
#     Extracts a chunk of text from 'inpt' text based on start and end keywords.
#     Example pattern: \d\.\sContingency([\s\S]*?)(?:\d\.\sAdjournment|\d\.\sSponsorship)
#     """
#     assert len(starts) != 0, 'starts is an empty list'
#     assert is_type(starts, str), 'starts is not a list of strings'
#     assert len(ends) != 0, 'ends is an empty list'
#     assert is_type(ends, str), 'ends is not a list of strings'
#     assert isinstance(end_prepattern, str), f'end_prepattern should be a string but is type {type(end_prepattern)}'
    
#     # Build the start pattern (matches but doesn't capture the first row)
#     if len(starts) == 1:
#         start_pattern = end_prepattern + starts[0]
#     else:
#         start_pattern = '(?:' + '|'.join(end_prepattern + s for s in starts) + ')'
    
#     # Build the end pattern
#     if len(ends) == 1:
#         end_pattern = end_prepattern + ends[0]
#     else:
#         end_pattern = '|'.join(end_prepattern + e for e in ends)
    
#     # Combine: start pattern, then capture everything until end pattern (using lookahead)
#     pattern = start_pattern + r'\s*([\s\S]*?)(?=' + end_pattern + ')'
    
#     return pattern

def _motion_processor(club_names, names_and_motions):
   """Takes in a list of club names for a given chunk and a list of club names and motions. Outputs a dictionary of club names mapped to keys containing the relevant motion. 
   club_names (list[str]): List of club names (eg. ['V-Day at Berkeley', 'Aion', 'Volunteer Income Tax Assistance Program', 'ASUC Menstrual Equity Commission', 'ASUC Menstrual Equity Commission'])
   names_and_motions (list[str]): List of club names and motions (eg. ['V-Day at Berkeley', 'Motion to approve $400 by Senator Manzoor', 'Seconded by Senator Ponna', 'Aion', 'Motion to approve $300 by Senator Manzoor ', 'Seconded by Senator Ponna ')
   """
   # print(f"Club Names: {club_names}")
   # print(f"Club Motions: {names_and_motions}")
   rv = {}
   repeats = dict(zip(club_names, [0]*len(club_names)))
   club_set = set(club_names)  
   curr_club = None
   for curr in names_and_motions: 
      if curr in club_set: 
         if curr in rv: #to register clubs that get repeated in the agenda due to multiple submissions
            curr_club = curr + f" ({str(repeats[curr] + 1)})"
         else:
            curr_club = curr
         rv[curr_club] = [] #to register clubs with no motions
      else: 
         if curr_club is None:
            print(f"""WARNING line skip occured with line: {curr}
            total list is: {names_and_motions}""")
         else:
            rv[curr_club].append(curr)

   return rv

def inpt_cleaner(inpt: str):
   inpt = re.sub(r"\(\$?\d+,?\d*\.?\d*\)", "", inpt)
   # Periods are already removed at the top level of Agenda_Processor
   return inpt

def _process_2020_nested_format(inpt: str, date: str, debug=True):
   """
   Processes 2020/2021 nested agenda format where sections are nested under 'Pending Business'.
   Structure: 4. Pending Business -> 1. Sponsorship / 2. Senate Contingency Funding -> Organizations
   Handles both Fall 2020 and Spring 2021 formats.
   """
   if debug:
      print("Attempting to process as 2020/2021 nested format...")

   # Define section mappings for 2020/2021 format
   section_map = {
      'Sponsorship': 'Sponsorship',
      'Senate Contingency Funding': 'Contingency',
      'Contingency Funding': 'Contingency',  # Spring 2021 uses this
      'Contingency': 'Contingency',
      'Funding': 'Contingency',  # Spring 2020 uses just "Funding"
      'Space Reservation': 'Space Reservation',
      'Finance Rule Waiver': 'Finance Rule',  # Spring 2021 uses full name
      'Finance Rule': 'Finance Rule',
      'Rule Waiver': 'Rule Waiver',
      'ABSA Appeals': 'ABSA Appeals'
   }

   list_of_dfs = []

   # Try to find and extract Pending Business section
   pending_pattern = r'Pending Business.*?(?=\d+\s+Adjournment|\d+\s+Guest|$)'
   pending_match = re.search(pending_pattern, inpt, re.DOTALL | re.IGNORECASE)

   if not pending_match:
      if debug:
         print("No Pending Business section found")
      return None

   pending_section = pending_match.group(0)
   if debug:
      print(f"Found Pending Business section (length: {len(pending_section)})")

   # Check if there's a Finance Rule subsection (FR 20/21 S##) that contains the actual sections
   # Some Spring 2021 files have this double-nested structure
   # Need to capture until we hit the next top-level item (minimal indent + number)
   fr_pattern = r'\d+\s+FR \d+/\d+ S\d+\s*\n(.*?)(?=\n\s{0,3}\d+\s+[A-Z]|$)'
   fr_match = re.search(fr_pattern, pending_section, re.DOTALL | re.IGNORECASE)
   if fr_match:
      # Use the FR subsection content as the base for extraction
      pending_section = fr_match.group(1)
      if debug:
         print(f"Found FR subsection, using it instead (length: {len(pending_section)})")

   # Process each subsection type
   for section_name, request_type in section_map.items():
      # Pattern to match section and its content until next section at same indentation level
      # Sections are numbered like "1. Sponsorship", "2. Senate Contingency Funding"
      # We want to capture everything until the next section at the same level (minimal indent + number)
      # More specific: capture until we hit another section name from our map
      other_sections = '|'.join([re.escape(s) for s in section_map.keys() if s != section_name])
      section_pattern = rf'(\d+)\s+{re.escape(section_name)}\s*\n(.*?)(?=\n\s{{0,8}}\d+\s+(?:{other_sections})|$)'
      section_match = re.search(section_pattern, pending_section, re.DOTALL | re.IGNORECASE)

      if not section_match:
         continue

      section_content = section_match.group(2)  # group 2 because group 1 is the section number
      if debug:
         print(f"\nFound section: {section_name}")
         print(f"Section content preview: {section_content[:200]}...")

      # Extract organizations (numbered items that are org names)
      # In 2020 format, orgs have specific indentation (usually 6 spaces)
      # Their sub-items have more indentation (9+ spaces)
      orgs = []
      org_contents = {}
      org_counts = {}  # Track duplicate org names

      lines = section_content.split('\n')
      current_org = None
      current_org_indent = 0

      for line in lines:
         # Check if this is a new organization
         # Single-nested (Fall 2020): 6 spaces for orgs, 9+ for motions
         # Double-nested (Spring 2021): 9 spaces for orgs, 12+ for motions
         # We'll try to detect both patterns
         org_match = re.match(r'^\s{6,10}(\d+)\s+(.+)', line)
         if org_match:
            indent = len(line) - len(line.lstrip())
            org_name = org_match.group(2).strip()

            # Only treat as org if not a motion/second/senator line or FR passing line
            if not re.match(r'(Motion|Second|Senator.*motions?\s+to\s+(pass|send)\s+FR)', org_name, re.IGNORECASE):
               # Handle duplicate org names by appending a count
               if org_name in org_counts:
                  org_counts[org_name] += 1
                  current_org = f"{org_name} ({org_counts[org_name]})"
               else:
                  org_counts[org_name] = 0
                  current_org = org_name

               current_org_indent = indent
               orgs.append(current_org)
               org_contents[current_org] = []
               if debug:
                  print(f"  Found org: {current_org}")
         elif current_org and line.strip():
            # This is content under the current org (motions, etc.)
            # Sub-items have deeper indentation than the org
            indent = len(line) - len(line.lstrip())
            if indent > current_org_indent:
               org_contents[current_org].append(line.strip())

      # Process decisions and allocations
      decisions = []
      allocations = []

      for org_name in orgs:
         motions = org_contents.get(org_name, [])
         sub_motions = " ".join(motions)

         if debug:
            print(f"  {org_name}: {sub_motions[:100]}")

         # Determine decision
         if re.search(r'(tabled?\sindefinetly)|(tabled?\sindefinitely)|(table\sindefinitely)|(deny)|not present.*tabled', sub_motions, re.IGNORECASE):
            decisions.append('Denied or Tabled Indefinetly')
            allocations.append(0)
         elif re.search(r'(tabled?\suntil)|(tabled?\sfor)|(tabled?\sto)|(table\sto)|(tabled to next week)|not present.*tabled', sub_motions, re.IGNORECASE):
            decisions.append('Tabled')
            allocations.append(0)
         elif re.search(r'(motion\s+(passes|passed|approved))|(motions?\s+to\s+(sponsor|approve|amend))', sub_motions, re.IGNORECASE):
            # Spring 2020 format: "motions to approve the waiver for $X" then "Motion passes/passed/approved"
            # Or: "motions to sponsor" then "Motion passes"
            # Or: "motions to amend the FR to allocate $X" then "Motion passes"

            # Check if motion actually passed/approved
            if re.search(r'motion\s+(passed|approved)', sub_motions, re.IGNORECASE):
               # Extract dollar amounts - check various patterns
               # Pattern 1: "waiver for $X"
               waiver_amount = re.findall(r'waiver\s+for\s+\$?(\d+(?:,\d+)?(?:\.\d+)?)', sub_motions, re.IGNORECASE)
               # Pattern 2: "allocate $X"
               allocate_amount = re.findall(r'allocate\s+\$?(\d+(?:,\d+)?(?:\.\d+)?)', sub_motions, re.IGNORECASE)
               # Pattern 3: "approve $X" or "approve X" (without dollar sign)
               approve_amount = re.findall(r'approve\s+(?:for\s+)?\$?(\d+(?:,\d+)?(?:\.\d+)?)', sub_motions, re.IGNORECASE)

               if waiver_amount:
                  decisions.append('Approved')
                  allocations.append(waiver_amount[0])
               elif allocate_amount:
                  decisions.append('Approved')
                  allocations.append(allocate_amount[0])
               elif approve_amount:
                  decisions.append('Approved')
                  allocations.append(approve_amount[0])
               else:
                  # Approved but no dollar amount (e.g., sponsorship)
                  decisions.append('Approved')
                  allocations.append(np.nan)
            else:
               # Motion was made but didn't pass
               decisions.append('ERROR could not find conclusive motion')
               allocations.append(np.nan)
         elif not sub_motions:
            decisions.append('No record on input doc')
            allocations.append(np.nan)
         else:
            decisions.append('ERROR could not find conclusive motion')
            allocations.append(np.nan)

      if orgs:
         df = pd.DataFrame({
            'Org Name': pd.Series(orgs).str.strip(),
            'Request Type': [request_type] * len(orgs),
            'Committee Status': decisions,
            'Amount': allocations,
            'Date': [date] * len(orgs),
         })
         list_of_dfs.append(df)
         if debug:
            print(f"Created DataFrame for {section_name} with {len(orgs)} organizations")

   if list_of_dfs:
      result = pd.concat(list_of_dfs, ignore_index=True)
      return result

   return None


def Agenda_Processor(inpt: str,
                     start=['Contingency', 'Finance Rule', 'Rule Waiver', 'Space Reservation'],
                     end=['Finance Rule', 'Rule Waiver', 'Space Reservation', 'Sponsorship', 'Adjournment', 'ABSA', 'ABSA Appeals'],
                     identifier=r'(?:\w+,\s)?(\w+\s\d{1,2}\w*,\s\d{4})',
                     date_format="%m/%d/%Y",
                     debug=True):
   """
   Processes agenda documents to extract funding information.
   Handles both modern format (flat sections) and 2020 format (nested under Pending Business).

   input (str): The raw text of the agenda to be processed. Usually a .txt file
   identifier (str): Regex pattern to extract a certain piece of text from inpt as the identifier for the chunk extracted from inpt
   """
   # Remove periods only from numbered list markers (e.g., "1." -> "1 "), preserving decimal numbers like "$43.72"
   # Pattern: digit(s) followed by period followed by whitespace (list markers)
   inpt = re.sub(r'(\d+)\.(\s)', r'\1\2', inpt)

   date_match = re.findall(rf"{identifier}", inpt)
   if not date_match:
      print(f"Agenda_Processor could not find date on agenda doc")
      date = "00/00/0000"
   else:
      date_str = date_match[0]  # the matched date string
      dt = pd.to_datetime(date_str, errors='coerce')  # parse string into timestamp object
      date = dt.strftime(date_format)
   # Check if this is a nested format (2020/2021 style with Pending Business)
   # If so, skip modern format processing and go straight to nested format
   has_pending_business = re.search(r'Pending Business', inpt, re.IGNORECASE)
   has_nested_sections = re.search(r'Pending Business.*?(Contingency Funding|Senate Contingency Funding|Finance Rule Waiver)', inpt, re.DOTALL | re.IGNORECASE)

   #Building key/value dictionary
   start_end_dict = {}
   for i in range(len(start)):
      start_end_dict[start[i]] = end[i:]
   list_of_dfs = []
   rv = None  # Initialize rv to avoid UnboundLocalError

   # Skip modern format if we detect nested structure
   if has_pending_business and has_nested_sections:
      if debug:
         print("Detected nested format (2020/2021), skipping modern format processing")
   else:
      for s in start_end_dict:
         if re.findall(rf"\d+\s{s}", inpt):


            pattern = _find_chunk_pattern(starts = [s], ends = start_end_dict[s])
            if debug:
               print(f"Agenda Processor Pattern: {pattern}")

            chunk = re.findall(rf"{pattern}", inpt)[0]
            chunk = inpt_cleaner(chunk)
            print(f"chunk: {chunk}")

            valid_name_chars = r'\w\s\-\_\*\&\%\$\+\#\@\!\(\)\,\'\"\[\]\.:' #seems to perform better with explicit handling for special characters? eg. for 'Telegraph+' we add the plus sign so regex will pick it up. Added \[\] for brackets and \. for periods in names like "Inc."
            club_name_pattern = f'\d+\s(?!Motion|Seconded)([{valid_name_chars}]+?)(?=\n)' #matches numbered items that are club names (not Motion/Seconded), capturing until newline
            club_names = list(re.findall(club_name_pattern, chunk)) #just matches club names --> list of tuples of club names
            if debug:
               print(f"Agenda Processor Club Names: {club_names}")

            names_and_motions = list(re.findall(rf'\d+\s(.+)\n?', chunk)) #pattern matches every single line that comes in the format "<digit><space><anything>""
            motion_dict = _motion_processor(club_names, names_and_motions)
            if debug:
               print(f"Agenda Processor Motion Dict: {motion_dict}")


            decisions = []
            allocations = []
            for name in motion_dict.keys():

               if motion_dict[name] == []:
                  decisions.append('No record on input doc')
                  allocations.append(np.nan)

               else:
                  sub_motions = " ".join(motion_dict[name]) #flattens list of string motions into one massive continuous string containing all motions
                  print(f'sub-motions: {sub_motions}')

                  #for handling multiple conflicting motions (which shouldn't even happen) we record rejections > temporary tabling > approvals > no input
                  #when in doubt assume rejection
                  #check if application was denied or tabled indefinetly
                  if re.findall(r'(tabled?\sindefinetly)|(tabled?\sindefinitely)|(deny)', sub_motions) != []:
                     decisions.append('Denied or Tabled Indefinetly')
                     allocations.append(0)
                  #check if the application was tabled
                  elif re.findall(r'(tabled?\suntil)|(tabled?\sfor)|(tabled?\sto)', sub_motions) != []:
                     decisions.append('Tabled')
                     allocations.append(0)
                  #check if application was approved and for how much
                  elif re.findall(r'[aA]pprove', sub_motions) != []:
                     # Check for partial approval first (more specific pattern)
                     # Handles: "partially approve $X" or "partially approve for $X"
                     partial_match = re.findall(r'[pP]artially\s+[aA]pprove\s+(?:for\s+)?\$?(\d+(?:,\d+)?(?:\.\d+)?)', sub_motions)
                     # Check for regular approval
                     dollar_amount = re.findall(r'[aA]pprove\s+(?:for\s+)?\$?(\d+(?:,\d+)?(?:\.\d+)?)', sub_motions)

                     if partial_match != []:
                        # Partially approved - use the amount specified after "partially approve"
                        decisions.append('Partially Approved')
                        allocations.append(partial_match[0])
                     elif dollar_amount != []:
                        decisions.append('Approved')
                        allocations.append(dollar_amount[0])
                     else:
                        decisions.append('Approved but dollar amount not listed')
                        allocations.append(np.nan) # not listed appends NaN
                  #check if there was no entry on ficomm's decision for a club (sometimes happens due to record keeping errors)
                  elif sub_motions == '':
                     decisions.append('No record on input doc')
                     allocations.append(np.nan)
                  else:
                     decisions.append('ERROR could not find conclusive motion')
                     allocations.append(np.nan)

            rv = pd.DataFrame({
               'Org Name' : pd.Series(motion_dict.keys()).str.strip(), #solves issue of '\r' staying at the end of club names and messing things up
               "Request Type": [s] * len(allocations),
               'Committee Status' : decisions,
               'Amount' : allocations,
               'Date' : [date]*len(allocations),
               }
            )

            list_of_dfs.append(rv)

   # Handle case where no matching sections were found
   if not list_of_dfs:
      if debug:
         print(f"Agenda Processor: No matching sections found in modern format")
         print(f"Attempting 2020 nested format processing...")

      # Try 2020 nested format
      rv = _process_2020_nested_format(inpt, date, debug=debug)

      if rv is None or rv.empty:
         if debug:
            print(f"Agenda Processor: No data found in any format")
         rv = pd.DataFrame(columns=['Org Name', 'Request Type', 'Committee Status', 'Amount', 'Date'])
      else:
         if debug:
            print(f"Successfully processed using 2020 nested format: {len(rv)} organizations found")
   else:
      if debug:
         print(f"Agenda Processor Final df: {rv}")
      rv = pd.concat(list_of_dfs)

   # Clean up organization names: remove asterisks and trailing commas/whitespace
   if not rv.empty:
      rv["Org Name"] = rv["Org Name"].str.replace("*", "", regex=False)
      rv["Org Name"] = rv["Org Name"].str.replace(r',\s*$', '', regex=True)
      rv["Org Name"] = rv["Org Name"].str.strip()

   return rv, date
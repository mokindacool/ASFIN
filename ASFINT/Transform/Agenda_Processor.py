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


def Agenda_Processor(inpt: str,
                     start=['Contingency', 'Finance Rule', 'Rule Waiver', 'Space Reservation'],
                     end=['Finance Rule', 'Rule Waiver', 'Space Reservation', 'Sponsorship', 'Adjournment', 'ABSA', 'ABSA Appeals'],
                     identifier='(\w+\s\d{1,2}\w*,\s\d{4})',
                     date_format="%m/%d/%Y",
                     debug=True):
   """
   You have a chunk of text from the document you want to turn into a table and an identifier for that chunk of text (eg. just the Contingency Funding section and the identifeir is the date).
   Thus function extracts the chunk and converts it into a tabular format.

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
   #Building key/value dictionary
   start_end_dict = {}
   for i in range(len(start)):
      start_end_dict[start[i]] = end[i:]
   list_of_dfs = []
   for s in start_end_dict:
      if re.findall(rf"\d+\s{s}", inpt):


         pattern = _find_chunk_pattern(starts = [s], ends = start_end_dict[s])
         if debug:
            print(f"Agenda Processor Pattern: {pattern}")

         chunk = re.findall(rf"{pattern}", inpt)[0]
         chunk = inpt_cleaner(chunk)
         print(f"chunk: {chunk}")

         valid_name_chars = r'\w\s\-\_\*\&\%\$\+\#\@\!\(\)\,\'\"\[\]\.' #seems to perform better with explicit handling for special characters? eg. for 'Telegraph+' we add the plus sign so regex will pick it up. Added \[\] for brackets and \. for periods in names like "Inc."
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
   print(f"Agenda Processor Final df: {rv}")
   rv = pd.concat(list_of_dfs)
   # Clean up organization names: remove asterisks and trailing commas/whitespace
   rv["Org Name"] = rv["Org Name"].str.replace("*", "", regex=False)
   rv["Org Name"] = rv["Org Name"].str.replace(r',\s*$', '', regex=True)
   rv["Org Name"] = rv["Org Name"].str.strip()

   return rv, date
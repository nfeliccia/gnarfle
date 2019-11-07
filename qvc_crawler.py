from bs4 import BeautifulSoup


class QVC_Job_Listing:
    def __init__(self):
        self.the_title = ""
        self.requisition_id = ""
        self.posted = ""
        self.country = ""
        self.state = ""
        self.city = ""


qvc_dump_file_name = r'F:\Qurate Retail Group - Workday.html'
with open(qvc_dump_file_name, 'r', encoding='utf-8') as qvc_dump_file:
    qvc_html = qvc_dump_file.read()

search_results_page_soup = BeautifulSoup(qvc_html, 'lxml')
high_level_listing = search_results_page_soup.find_all('div', class_="WF-F WD-F")

for listing in high_level_listing:
    the_title = listing.find('div', class_="gwt-Label WM1O WG0O").string
    the_second_line = listing.find('span', class_="gwt-InlineLabel WI0F WH-F")
    # Parse out the second line.
    the_second_line_split = the_second_line.string.split('|')
    the_first_part = the_second_line_split[0]
    requisition = the_second_line_split[1].strip()
    when_posted = the_second_line_split[2].strip()
    the_address_elements = the_first_part.split(',')
    country = the_address_elements[0]
    try:
        state = the_address_elements[1].strip()
        city = the_address_elements[2].strip()
    except IndexError:
        state = "Unknown"
        city = "Unkown"
    print(the_title)
    print(requisition, when_posted)
    print(country, state, city)
    print("########################")

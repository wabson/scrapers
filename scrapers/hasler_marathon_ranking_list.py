import datetime
import lxml.html
import re
import scraperwiki
import sys
import xlrd

keys = [ 'surname', 'first_name', 'club', 'cat', 'bcu_number', 'expiry', 'div' ]
translated_keys = [ 'surname', 'first_name', 'club', 'class', 'bcu_number', 'expiry', 'division' ]
unique_keys = [ 'surname', 'first_name', 'club', 'class' ]
rankings_page = 'http://canoeracing.org.uk/marathon/index.php/latest-marathon-ranking-list/'

def get_rankings_page():
    return scraperwiki.scrape(rankings_page)

def get_xls_url(r_html):
    m = re.compile('<a href="(http://canoeracing.org.uk/marathon/wp-content/uploads/20\\d{2}/\\d{2}/RankingList[\w\\-]+.xls)">').search(r_html)
    if m is not None:
        return m.group(1)
    return None

def main():
    #xlbin = scraperwiki.scrape("http://dl.dropboxusercontent.com/u/22425821/RankingList.xls")
    #xlbin = scraperwiki.scrape("http://www.marathon-canoeing.org.uk/marathon/media/RankingList.xls")
    #print scraperwiki.sqlite.show_tables()
    xls_url = get_xls_url(get_rankings_page())
    if xls_url is None:
        raise Exception('Could not find rankings sheet link on %s' % (rankings_page))
    xlbin = scraperwiki.scrape(xls_url)

    book = xlrd.open_workbook(file_contents=xlbin)
    sheet = book.sheet_by_index(0)
    datarows = []

    columns = get_columns(sheet)
    if not check_columns(columns):
        raise Exception('Column list not as expected!')
    translated_columns = translate_column_names(get_columns(sheet))

    # As of 07/15 last updated date is not in the spreadsheet
    #updated = cellval(get_last_updated(sheet), book.datemode)
    updated = datetime.date.today()
    updated_str = '%s-%s-%s' % (updated.year, updated.month, updated.day)

    for rownumber in range(1, sheet.nrows):
        # create dictionary of the row values
        values = [ cellval(c, book.datemode) for c in sheet.row(rownumber) ][0: len(translated_columns)]

        # Only save if there are enough values
        if len(values) == len(translated_columns):
            data = filter_row_data(dict(zip(translated_columns, values)))
            data['updated'] = updated_str

            # Division/BCU number seem to be read as a real number when numeric (e.g. 4.0 not 4)
            if data['division'] is not None and isinstance(data['division'], float):
                data['division'] = str(int(data['division']))
            if data['bcu_number'] is not None and isinstance(data['bcu_number'], float):
                data['bcu_number'] = str(int(data['bcu_number']))

            # Set club to empty string value (cannot be None since it is part of the unique key)
            if data['club'] is None:
                data['club'] = ''

            # only save if it is a full row (rather than a blank line or a note)
            if data['surname'] != None and data['first_name'] != None:
                datarows.append(data)
                #print data
                #scraperwiki.sqlite.save(unique_keys=unique_keys, data=[data])
            else:
                print 'WARNING: Not enough values on row %s' % (rownumber)
        else:
            print 'WARNING: Not enough values on row %s' % (rownumber)
    scraperwiki.sqlite.save(unique_keys=unique_keys, data=datarows)

    scraperwiki.sqlite.save_var('last_updated', updated_str)

def check_columns(columns):
    return all([ k in columns for k in keys ])

def get_columns(sheet):
    return [c.strip().lower().replace(' ', '_') for c in sheet.row_values(0)]

def translate_column_names(raw):
    return [ k in keys and translated_keys[keys.index(k)] or k for k in raw ]

def filter_row_data(input):
    return dict((k, v) for k, v in input.items() if k in translated_keys)

def get_last_updated(sheet):
    return sheet.cell(0,len(keys))

def cellval(cell, datemode):
    if cell.ctype == xlrd.XL_CELL_DATE:
        datetuple = xlrd.xldate_as_tuple(cell.value, datemode)
        if datetuple[3:] == (0, 0, 0):
            return datetime.date(datetuple[0], datetuple[1], datetuple[2])
        return datetime.date(datetuple[0], datetuple[1], datetuple[2], datetuple[3], datetuple[4], datetuple[5])
    if cell.ctype == xlrd.XL_CELL_EMPTY:
        return None
    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        return cell.value == 1
    return cell.value

main()

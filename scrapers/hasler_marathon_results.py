import scraperwiki
import getopt
import lxml.html
import lxml.etree
import re
import urllib2
import unittest
import sys

from datetime import date
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.sql import select, and_

data = { 'races': [], 'results': [], 'club_points': [] }
batch_size = 100

#scraperwiki.sqlite.execute("create table club_points ('hasler_year' int, 'race_url' string, 'race_name' string, 'club_name' string, 'position' int, 'points' int)")
#print scraperwiki.sqlite.execute("delete from races where race_date LIKE '%2011'")
#scraperwiki.sqlite.commit()

DB_CLUB_LIST='hasler_marathon_club_list'

#base_url = 'http://www.marathon-canoeing.org.uk/marathon/results/'
base_url = 'http://www.canoeracing.org.uk/marathon/results/'
years = [date.today().year]
dry_run = False
verbose = False

keys = {
    'results': [ 'boat_number', 'name_1', 'club_1', 'class_1', 'points_1', 'p_d_1', 'bcu_number_1', 'name_2', 'club_2', 'class_2', 'points_2', 'p_d_2', 'bcu_number_2', 'race_name', 'race_division', 'position', 'retired', 'time' ],
    'races': [ 'race_name', 'race_title', 'race_date', 'results_url', 'region', 'hasler_year' ],
    'club_points': [ 'hasler_year', 'race_url', 'race_name', 'club_name', 'position', 'points' ]
}
unique_keys = {
    'results': ['race_name', 'race_division', 'boat_number'],
    'races': [ 'results_url' ],
    'club_points': [ 'hasler_year', 'race_url', 'club_name' ]
}
table_names = { 'results': 'results', 'races': 'races', 'club_points': 'club_points' }

skip_races=0

result_url_overrides = { '2008/Richmond2008.htm': 'http://www.richmondcanoeclub.com/documents/2008/hrmTemplate2richmond2008_scroll.htm',
#    '2008/Chester2_2008.htm': 'http://chestercanoeclub.org.uk/chester_2_2008.htm'
#     '2012/Maidstone2012.htm': 'http://www.maidstonecanoeclub.net/joomla/index.php/racing/maidstone-marathon/results/71-maidstone-marathon-results-2012-provisional',
#     '2012/Hastings2012.htm': 'http://www.hastingscanoeclub.org.uk/recent-events-results/192-1066-marathon-results-2012'
#    '2012/Royal2012.htm': 'http://www.royalcanoeclub.com/wp-content/uploads/2012/06/Hasler2012-results.htm',
#    '2012/Richmond2012.htm': 'http://richmondcanoeclub.com/wp-content/uploads/2012/07/Richmond-Hasler-2012-Results.htm'
}

extra_results = [
#    ('2012/Richmond2012.htm', 'Richmond', '08/07/2012')
]

declared_club_points = []
club_points_k1 = {}
club_points_k2 = {}
club_names = {} # Cache of recently looked-up club names
club_codes = {} # Cache of recently looked-up club codes

def delete_race_data(url):
    print "DELETE FROM club_points WHERE race_url = '" + url + "';"
    print "DELETE FROM results WHERE race_name = '" + url + "';"
    print "DELETE FROM races WHERE results_url = '" + url + "';"

def main():
    #delete_race_data('2012/Richmond2012.htm')
    for year in years:
        try:
            races_url = '%sResults%s.html' % (base_url, year)
            races = get_races(races_url)
            races.extend(extra_results)
            scrape_races_html(races)
        except urllib2.HTTPError, e:
            if e.code == 404:
                print "Missing year %s" % (year)
            else:
                raise e
    #scrape_results_html('http://www.hastingscanoeclub.org.uk/recent-events-results/192-1066-marathon-results-2012', 'Hastings 1066', '20/05/2012')
    #scrape_results_html('http://www.maidstonecanoeclub.net/joomla/index.php/racing/maidstone-marathon/results/71-maidstone-marathon-results-2012-provisional', 'Maidstone', '17/06/2012')
    #delete_race_data('http://www.maidstonecanoeclub.net/joomla/index.php/racing/maidstone-marathon/results/71-maidstone-marathon-results-2012-provisional')
    #delete_race_data('http://www.hastingscanoeclub.org.uk/recent-events-results/192-1066-marathon-results-2012')
    #scrape_results_html('2012/Windsor2012.htm', 'Windsor', '14/10/2012')
    #scrape_results_html('2007/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2008/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2009/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2010/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2011/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2012/HaslerFinal2007.htm', '', '')
    #scrape_results_html('2013/Calder2013.htm', 'Calder', '16/06/2013')
    #scrape_results_html('http://www.royalcanoeclub.com/wp-content/uploads/2015/02/Royal-Hasler-2015-hrm.htm', 'Royal', '21/06/2015')
    #scrape_results_html('http://tccblog.tonbridgecanoeclub.org.uk/wp-content/uploads/2015/08/TONResults15.htm', 'Tonbridge', '29/08/2015')
    #scrape_results_html('http://wabson.org/Longridge2015.htm', 'Longridge', '30/08/2015')
    print "Finished"

def scrape(url):
    user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
    headers = { 'User-Agent' : user_agent }
    data = None
    req = urllib2.Request(url, data, headers)
    response = urllib2.urlopen(req)
    return response.read()

def get_races(races_url):
    races = []
    race_html = lxml.html.fromstring(scrape(races_url))
    race_rows = race_html.cssselect('table tr')[skip_races:]
    #print "Found %s rows" % (len(race_rows))
    for n in range(len(race_rows)):
        td_els = race_rows[n].findall('td')
        if len(td_els) == 2:
            #print "%s. %s" % (n, td_els[1].text_content().strip())
            race_date = td_els[0].text_content().strip()
            link = td_els[1].find('a')
            if link is not None:
                race_name = link.text_content().strip()
                race_path = link.get('href')
                if race_path is not None and race_path.endswith(('.htm', '.html')):
                    races.append((race_path, race_name, race_date))
                else:
                    print 'WARNING: Missing race link' if race_path is None else 'WARNING: Skipping bad (non-HTML) race link %s' % (race_path)
    return races

def scrape_races_html(races):
    for r in races:
        scrape_results_html(r[0], r[1], r[2])

def scrape_results_html(race_path, race_name='', race_date=''):
    global declared_club_points
    global club_points_k1
    global club_points_k2
    # Allow race URL to be overridden (e.g. results only posted on club website, not marathon site)
    race_url = ('%s%s' % (base_url, race_path) if race_path not in result_url_overrides else result_url_overrides[race_path]) if not race_path.startswith('http') else race_path
    if verbose:
        print race_url
    try:
        # Must remove </body>\n</html>\n<html>\n<body> lines in middle of the document
        results_html = lxml.html.fromstring(re.sub(r'</tr>\s*<td>', '</tr><tr><td>', re.sub(r'\s</body>\s</html>\s<html>\s<body>', '', scrape(race_url).replace('UTF-8', 'iso-8859-1'))))
        h1_el = results_html.find('*/h1')
        if h1_el is None:
            h1_el = results_html.find('*/H1')
        # Older template uses h2
        if h1_el is None:
            h1_el = results_html.find('*/h2')
        if h1_el is None:
            h1_el = results_html.find('*/H2')
        race_title = re.sub('Results\:? ', '', h1_el.text_content().strip()) if h1_el is not None else ''
        date_arr = race_date.split('/')
        club_points_k1 = {}
        club_points_k2 = {}
        declared_club_points = []
        
        for table_el in results_html.cssselect('table'):
            caption_el = table_el.find('caption') if table_el.find('caption') is not None else table_el.find('CAPTION')
            if caption_el is not None:
                div_name = caption_el.text_content().strip()
                boat_num = 0
                hdr_names = []
                r_th_els = table_el.cssselect('tr th')
                for r_tr_el in table_el.cssselect('tr'):
                    hdr_names = [ get_result_value(thel).lower() for thel in r_th_els ]
                    r_td_els = r_tr_el.cssselect('td')
                    if div_name == 'Club points':
                        #print 'Saving club points'
                        if len(r_td_els) == 3:
                            data_row = dict(zip(hdr_names[0:len(r_td_els)], get_row_values(r_td_els)))
                            declared_club_points.append({
                                'hasler_year': get_hasler_end_year(date(int(date_arr[2]), int(date_arr[1]), int(date_arr[0]))),
                                'race_url': race_path,
                                'race_name': race_name,
                                'club_name': get_club_code(data_row['club']) or data_row['club'],
                                'points': data_row['points'],
                                'position': data_row['overall']
                            })
                    else:
                        if len(r_td_els) >= 5:
                            boat_num += 1
                            data_row = dict(zip(hdr_names[0:len(r_td_els)], get_row_values(r_td_els)))
                            position = data_row['position'] if 'position' in data_row else None
                            names = data_row['name'] if 'name' in data_row else None
                            clubs = data_row['club'] if 'club' in data_row else None
                            classes = data_row['class'] if 'class' in data_row else None
                            divs = data_row['div'] if 'div' in data_row else None
                            pd = data_row['p/d'] if 'p/d' in data_row else None
                            time = data_row['time'] if 'time' in data_row else None
                            points = data_row['points'] if 'points' in data_row else None
                            rtd = False
                            if time == 'rtd':
                                rtd = True
                                time = ''
    
                            if names is None or position is None or clubs is None or classes is None:
                                raise Exception("Mandatory result data was not found")

                            if div_name.startswith('Div') and points is not None:
                                if (isinstance(clubs, list)):
                                    for i in [0,1]:
                                        if len(points) > i and points[i] is not None and points[i] != "":
                                            add_club_points(clubs[i], int(points[i]), 'k1' if len(names) == 1 else 'k2')
                                else:
                                    if points is not None and points != "":
                                        add_club_points(clubs, int(points), 'k1' if len(names) == 1 else 'k2')
    
                            # Save result data
                            save_data({'results': {
                                'boat_number': boat_num, 
                                'name_1': (names[0] if (isinstance(names, list)) else names), 
                                'club_1': (clubs[0] if (isinstance(clubs, list)) else clubs), 
                                'class_1': (classes[0] if (isinstance(classes, list)) else classes), 
                                'div_1': (divs[0] if (divs is not None and isinstance(divs, list)) else divs),
                                'points_1': (int(points[0] or 0) if (points is not None and isinstance(points, list)) else int(points or 0)), 
                                'p_d_1': (pd[0] if (pd is not None and isinstance(pd, list)) else pd), 
                                'bcu_number_1': None, 
                                'name_2': (names[1] if (isinstance(names, list) and len(names) > 1) else None), 
                                'club_2': (clubs[1] if (isinstance(clubs, list) and len(clubs) > 1) else None), 
                                'class_2': (classes[1] if (isinstance(classes, list) and len(classes) > 1) else None), 
                                'div_2': (divs[1] if (divs is not None and isinstance(divs, list) and len(divs) > 1) else None), 
                                'points_2': (int(points[1] or 0) if (points is not None and isinstance(points, list) and len(points) > 1) else 0), 
                                'p_d_2': (pd[1] if (pd is not None and isinstance(pd, list) and len(pd) > 1) else None), 
                                'bcu_number_2': None, 
                                'race_name': race_path, 
                                'race_division': div_name, 
                                'position': position, 
                                'retired': rtd, 
                                'time': time
                            }})

        # Save club points if they have not been saved yet
        positions = get_club_positions()
        if len(positions) > 0:
            if len(declared_club_points) == 0:
                print 'Warning: Could not find club points listed for race %s, will auto-calculate' % (race_name)
            else:
                if len(declared_club_points) != len(positions):
                    print "** ERROR: Club points size %s does not match calculated points size %s" % (len(declared_club_points), len(positions))
                    print "Stated:"
                    for i in range(0, len(declared_club_points)):
                        print "%s %s %s" % (declared_club_points[i]['club_name'], declared_club_points[i]['points'], declared_club_points[i]['position'])
                    print "Calculated:"
                    for i in range(0, len(positions)):
                        print "%s %s %s" % (positions[i]['code'],  positions[i]['points'],  positions[i]['position'])
                elif not all([ ((int(declared_club_points[i]['position']) == int(positions[i]['position']))) and (int(declared_club_points[i]['points']) == int(positions[i]['points'])) for i in range(0, len(positions)) ]):
                    print "** ERROR: Club points %s do not match calculated points %s" % (declared_club_points, positions)
                    print "Stated:"
                    for i in range(0, len(declared_club_points)):
                        print "%s %s %s" % (declared_club_points[i]['club_name'], declared_club_points[i]['points'], declared_club_points[i]['position'])
                    print "Calculated:"
                    for i in range(0, len(positions)):
                        print "%s %s %s" % (positions[i]['code'],  positions[i]['points'],  positions[i]['position'])

            date_arr = race_date.split('/')
            hasler_year = get_hasler_end_year(date(int(date_arr[2]), int(date_arr[1]), int(date_arr[0].split('-')[0])))
            for cp in positions:
                points_data = {
                    'hasler_year': hasler_year,
                    'race_url': race_path,
                    'race_name': race_name,
                    'club_name': cp['code'],
                    'points': cp['points'],
                    'position': cp['position']
                }
                if not dry_run:
                    save_data({'club_points': points_data})
                else:
                    print points_data

        # Save race data
        sclubs = get_scoring_clubs()
        regions = scoring_regions(sclubs)
        region_id = len(regions) == 1 and regions.pop() or None
        if region_id is None:
            if regions == set(['SCOE', 'SCOW']) and int(date_arr[2]) < 2014:
                region_id = 'SCO'
            else:
                print 'WARNING: Could not determine region for %s (Points scored for %s in regions %s)' % (race_name, ', '.join(sclubs), ', '.join(regions))
        hasler_year = race_name == 'Hasler Final' and int(date_arr[2]) or get_hasler_end_year(date(int(date_arr[2]), int(date_arr[1]), int(date_arr[0].split('-')[0])))
        # save race
        #scraperwiki.sqlite.save(unique_keys=races_unique_keys, data=dict(zip(races_keys, [race_name, race_title, race_date, race_path])), table_name=races_table_name, verbose=data_verbose)
        # Save race data
        if not dry_run:
            save_data({'races': dict(zip(keys['races'], [race_name, race_title, '%s-%s-%s' % (date_arr[2], date_arr[1], date_arr[0]), race_path, region_id, hasler_year]))})
        else:
            print ', '.join([race_name, race_title, '%s-%s-%s' % (date_arr[2], date_arr[1], date_arr[0]), race_path, str(region_id) , str(hasler_year)])

        # Flush all results in this division and the race itself to the datastore
        print "Saving %s results for %s" % (len(data['results']), race_name)
        if not dry_run:
            save_data(items={'races': None, 'results': None, 'club_points': None}, force=True)
                
    except urllib2.HTTPError, e:
        if e.code == 404:
            print 'WARNING: Missing data for %s' % (race_name)
        else:
            raise e

def get_row_values(tdels):
    return [ (get_result_values(el) if '<br' in lxml.etree.tostring(el).lower() else get_result_value(el)) for el in tdels ]

def get_result_values(tdel):
    return re.sub('\s*<[bB][rR] */?>\s*', '|', re.sub('\s*</?[tT][dD][^>]*>\s*', '', re.sub('&#160;?', ' ', re.sub('&nbsp;?', ' ', (lxml.etree.tostring(tdel) or ''))))).replace('&#13;', '').strip().split('|')

def get_result_value(tdel):
    return tdel.text_content().replace('&#13;', '').strip()

# The Hasler season runs from 1st September to 31st August each year. This gives the year in which the Hasler season for the given date finishes, i.e. when the finals are held.
def get_hasler_end_year(date):
    return int(date.year) if date.month < 9 else int(date.year + 1)

def add_club_points(club_code, points, entrytype):
    if entrytype == 'k1':
        if club_code not in club_points_k1:
            club_points_k1[club_code] = []
        club_points_k1[club_code].append(points)
    elif entrytype == 'k2':
        if club_code not in club_points_k2:
            club_points_k2[club_code] = []
        club_points_k2[club_code].append(points)
    else:
        raise Exception('Bad boat type %s' % (entrytype))

def get_total_points():
    points = {}
    for k, v in club_points_k1.items():
        points[k] = v
    for k, v in club_points_k2.items():
        points[k] = points.get(k, []) + v
    return points

def get_club_positions(hasler_final=False):
    positions = []
    lastpoints = 9999
    lastpos = 11
    nextpos = 10
    allpoints = get_total_points()
    items = get_club_total_points(allpoints)
    for p in items:
        pos = lastpos if p[1] == lastpoints else nextpos
        club_name = get_club_name(p[0])
        # Skip clubs which are not found in the database
        if club_name is not None:
            nextpos = nextpos - 1
            positions.append({'code': p[0], 'name': club_name, 'points': p[1], 'position': pos if pos > 0 else 1}) # all clubs taking part seem to get 1 point
            lastpos = pos
            lastpoints = p[1]
        else:
            print 'WARNING: Skipping unknown club %s in points calculations' % (p[0])
    return positions

def get_club_total_points(club_points, club_points_k2=None):
    def compare(a, b):
        return cmp(b[1], a[1]) # compare points
    byclub = dict(zip(club_points.keys(), [club_points[k].sort(reverse=True) or sum(club_points[k][0:12]) for k in club_points.keys()]))
    items = byclub.items()
    items.sort(compare)
    return items

def get_club_db_url():
    return 'sqlite:///db/%s.sqlite' % (DB_CLUB_LIST)

def get_club(colname, code, cache):
    cols = ['code', 'name', 'region_code']
    club = cache and cache.get(code, None) or None
    if club is None:
        eng = create_engine(get_club_db_url())
        with eng.connect() as con:
            meta = MetaData(eng)
            clubs = Table('swdata', meta, autoload=True)
            stm = select([getattr(clubs.c, col) for col in cols]).where(getattr(clubs.c, colname) == code)
            rows = con.execute(stm).fetchall()
            club = dict(zip(cols, rows[0])) if len(rows) == 1 else None
            if cache is not None and club is not None:
                cache[code] = club
    return club

def get_club_name(code):
    global club_names
    club = get_club('code', code, club_names)
    return club and club['name'] or None

def get_club_code(name):
    global club_codes
    club = get_club('name', name, club_codes)
    return club and club['code'] or None

def get_club_data():
    cols = ['code', 'name', 'region_code']
    eng = create_engine(get_club_db_url())
    with eng.connect() as con:
        meta = MetaData(eng)
        clubs = Table('swdata', meta, autoload=True)
        stm = select([getattr(clubs.c, col) for col in cols])
        rows = con.execute(stm).fetchall()
        return [ dict(zip(cols, row)) for row in rows ]

def get_scoring_clubs():
    return set(club_points_k1.keys() + club_points_k2.keys())

def scoring_regions(scoring_clubs):
    return set([ c['region_code'] for c in get_club_data() if c['code'] in scoring_clubs ])

def save_data(items={}, force=False):
    global data
    for k in items.keys():
        if items[k] is not None:
            data[k].append(items[k])
        if len(data[k]) >= batch_size or force == True:
            scraperwiki.sqlite.save(unique_keys=unique_keys[k], data=data[k], table_name=table_names[k])
            data[k] = []

def usage():
    print 'Usage: hasler_marathon_results.py test'
    print 'Usage: hasler_marathon_results.py [--dry-run] [--url=] [--name=] [--date=] [--years=]'

class TestFunctions(unittest.TestCase):

    def test_get_club_name(self):
        self.assertEqual(get_club_name('RIC'), 'Richmond CC')
        self.assertEqual(get_club_name('doesnotexist'), None)
        self.assertEqual(len(club_names), 1)

    def test_get_club_code(self):
        self.assertEqual(get_club_code('Richmond CC'), 'RIC')
        self.assertEqual(get_club_name('doesnotexist'), None)
        self.assertEqual(len(club_codes), 1)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        del sys.argv[1:]
        unittest.main()
    else:
        try:
            opts, args = getopt.getopt(sys.argv[1:], 'hv', ['help', 'verbose', 'dry-run', 'url=', 'name=', 'date=', 'years='])
        except getopt.GetoptError as err:
            # print help information and exit:
            print str(err) # will print something like "option -a not recognized"
            usage()
            sys.exit(2)
        race_url = None
        race_name = ''
        race_date = ''
        for o, a in opts:
            if o in ('-v', '--verbose'):
                verbose = True
            elif o in ('-h', '--help'):
                usage()
                sys.exit()
            elif o == '--dry-run':
                dry_run = True
            elif o == '--url':
                race_url = a
            elif o == '--name':
                race_name = a
            elif o == '--date':
                race_date = a
            elif o == '--years':
                years = a.split(',')
            else:
                print 'unhandled option %s' % (o)
                usage()
                sys.exit(2)
        if (race_url is not None):
            scrape_results_html(race_url, race_name, race_date)
        else:
            main()

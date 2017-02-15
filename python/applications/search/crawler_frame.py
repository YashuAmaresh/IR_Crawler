import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time

try:
    # For python 2
    from urlparse import urlparse, parse_qs, urljoin
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 3000

num_invalid_links = 0
max_outlinks = 0
max_outlinks_url = ""
subdomains_visited = dict()
already_visited = set()

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):   

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "31721795_50924931_85241493"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "IR W17 Grad 31721795 50924931 85241493"
        
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        f = open("Analytics", "w")
        f.write("Subdomains visited and their count:\n")
        for key in subdomains_visited:
            f.write(key + " : " + str(subdomains_visited[key]) + "\n")
        f.write("\n\n")
        f.write("Number of invalid links received: " + str(num_invalid_links) + "\n")
        f.write("Page with most outlinks: " + str(max_outlinks_url) + " - " + str(max_outlinks) + " number of outlinks.\n")
        f.close()

        print "downloaded ", len(url_count), " in ", time() - self.starttime, " seconds."
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    global max_outlinks
    global max_outlinks_url

    outputLinks = list()
    from bs4 import BeautifulSoup
    from lxml.html import soupparser

    #print rawDatas
    # with open ( "output001.txt" , "a" ) as op:
    for data in rawDatas:
        curr_url = data.url
        htmlStr = data.content
        if data.is_redirected:
            curr_url = data.final_url

        #print curr_url, htmlStr
        # op.write ( "Output base url = %s\n" % curr_url ) 
        # op.write("\n")
        if htmlStr and htmlStr.strip() != "":

            # BeautifulSoup(htmlStr, "html.parser")

            '''
            Using BeautifulSoup Parser to auto-detect the encoding of the HTML content
            '''

            root = html.fromstring(htmlStr)
            
            try:
                
                ignore = html.tostring(root, encoding='unicode')

            except UnicodeDecodeError:
                root = html.soupparser.fromstring(htmlStr)
            # dom = soupparser.fromstring(htmlStr)
            # dom =  html.fromstring(htmlStr)
            # print dom.xpath('//a/@href')
            # for link in root.xpath('//a/@href'): # select the url in href for all a tags(links)
            #     print link
                # op.write("Link = %s"% link + '\n')
            links = root.xpath('//a/@href')
            absoluteLinks = convertToAbsolute(curr_url, links)
            result = set(absoluteLinks)


            if len(result) > max_outlinks:
                max_outlinks = len(result)
                max_outlinks_url = curr_url

            outputLinks.extend(result)
                
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    global num_invalid_links
    global already_visited

    

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return_val = True

        return_val = ".ics.uci.edu" in parsed.hostname \
        and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
        + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
        + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
        + "|thmx|mso|arff|rtf|jar|csv"\
        + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

       # print "Parsed hostname : ", parsed.hostname, " : "
        '''
        Check here for crawler traps
        '''
        if(return_val): #Only if the url is valid check for the traps
            
            if parsed.netloc.lower() + "/" + parsed.path.lower().lstrip("/") in already_visited:
                print "Already visited"
                return_val = False

            #Add the current URL path to set of already visited paths 
            else: 
                print "Not yet visited"
                already_visited.add(parsed.netloc.lower() + "/" + parsed.path.lower().lstrip("/"))
                return_val = True

            # 1. Repeating directories
            if re.match("^.*?(/.+?/).*?\\1.*$|^.*?/(.+?/)\\2.*$", parsed.path.lower()):
                print "In Repeating Directories"
                print url
                return_val = False        

            # 2. Crawler traps - Keep track of already visited paths
            # elif parsed.netloc.lower() + "/" + parsed.path.lower().lstrip("/") in already_visited:
            #     return_val = False

            # #Add the current URL path to set of already visited paths 
            # else: 
            #     already_visited.add(parsed.netloc.lower() + "/" + parsed.path.lower().lstrip("/"))
            #     return_val = True

            if "archive.ics.uci.edu" in parsed.netloc.lower():
                return_val = False
        else:
            print "URL out of domain or is a non-crawlable file type: ", url

        if not return_val:  #Counting invalid links
            num_invalid_links += 1

        print return_val
        return return_val

    except TypeError:
        print ("TypeError for ", parsed)


def convertToAbsolute(url, links):
    '''
        <scheme>://<username>:<password>@<host>:<port>/<path>;<parameters>?<query>#<fragment>
        Not handled mailto and fragments(#)
        Also, javascript needs to be handled
    '''

    global subdomains_visited
    parsed_url = urlparse(url)

    # To maintain a dict of subdomains visted
    subdomains_visited[parsed_url.netloc] = subdomains_visited.get(parsed_url.netloc, 0) + 1

    base_url = parsed_url.scheme +"://"+ parsed_url.netloc + parsed_url.path
    absolutelinks = list()
    # with open("output101.txt","a") as op:
    for link in links:
        link = link.strip()

        if link.find('http') == 0 and is_valid(link):
            print "Absolute = " + link 
            absolutelinks.append(link)

        elif link.find('//') == 0 and is_valid(link):
            print "Second Absolute = " + link
            absolutelinks.append(link)

        elif link.find('#') == 0 or link.find("javascript") == 0 or link.find("mailto") == 0: #****
            print "#"
            pass

        # elif link.find("/") == 0:
        #     url_given = parsed_url.path.lower().strip().rstrip("/")
        #     if re.match(".*\.(asp|aspx|axd|asx|asmx|ashx|css|cfm|yaws|swf|html|htm|xhtml" \
        #         + "|jhtmljsp|jspx|wss|do|action|js|pl|php|php4|php3|phtml|py|rb|rhtml|shtml|xml|rss|svg|cgi|dll)$", url_given):
        #         # print "\n\n\n\nHere\n\n\n\n"

        #         index = url_given.rfind("/")
        #         parent_path = parsed_url.path[:index]

        #         print "URL: ", parsed_url.netloc, " : ", parsed_url.path, " -> ", parent_path, "-> ", link
        #         result = parsed_url.scheme +"://"+ parsed_url.netloc + parent_path + link
        #         print "Case3", result
        #     else:
        #         result = parsed_url.scheme +"://"+ parsed_url.netloc + parsed_url.path.rstrip("/") + link
        #         print "Case3 Else" 

        #     if(is_valid(result)):
        #         print "Case 3 " + result
        #         absolutelinks.append(result)

        else:
            
            result = urljoin(base_url,link)
            if(is_valid(result)):
                print "Else = " + result
                absolutelinks.append(result)
    
    print base_url
    # op.write("Base_url = " + base_url + '\n' )
    for urls in absolutelinks:
        print "Link= " + urls +"\n"
        # op.write("Link= " +urls +'\n')
    return absolutelinks



def is_absolute_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    # global num_invalid_links
    # global already_visited

    

    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return_val = True

        return_val = ".ics.uci.edu" in parsed.hostname \
        and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
        + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
        + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
        + "|thmx|mso|arff|rtf|jar|csv"\
        + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

       # print "Parsed hostname : ", parsed.hostname, " : "
        '''
        Check here for crawler traps
        '''
        if(return_val): #Only if the url is valid check for the traps

            # 1. Repeating directories
            if re.match("^.*?(/.+?/).*?\\1.*$|^.*?/(.+?/)\\2.*$", parsed.path.lower()):
                print "In Repeating Directories"
                print url
                return_val = False       

            if "archive.ics.uci.edu" in parsed.netloc.lower():
                return_val = False

        else:
            print "URL out of domain: ", url

        print return_val
        return return_val

    except TypeError:
        print ("TypeError for ", parsed)

from multiprocessing import Process, Queue, current_process
import requests
from bs4 import BeautifulSoup
from random import randint
import sys
#sys.setrecursionlimit(1000)

class Spider(object):
    """docstring for Spider"""

    # define a example function
    @staticmethod
    def rand_string(length, output):

        print("{} entry point".format(current_process().name))
        random_post=randint(1000000,9999999)
        response=requests.get('https://stackoverflow.com/questions/'+str(random_post))
        print("{} got request response".format(current_process().name))
        soup=BeautifulSoup(response.content,'lxml')
        try:
            title = soup.find('a',{'class':'question-hyperlink'}).string
        except:
            title = "not found"

        print("{} got title: '{}' of type: {}".format(current_process().name, title, type(title)))

        ###### This did it ######
        title = str(title) #fix or fake news?

        output.put([title,current_process().name])
        output.close()
        print("{} exit point".format(current_process().name))


    # Setup a list of processes that we want to run
#    @staticmethod
    def run(self, outq):
        processes = []
        for x in range(5):
                processes.append(Process(target=self.rand_string, name="process_{}".format(x), args=(x, outq,),) )
                print("creating process_{}".format(x))

        for p in processes:
            p.start()
            print("{} started".format(p.name))

        # Exit the completed processes
        for p in processes:
            p.join()
            print("successuflly joined {}".format(p.name))

        # Get process results from the output queue
        print("joined all workers")
#        return None
        out = []
        while not outq.empty():
            result = outq.get()
            print("got {}".format(result))
            out.append(result)
        return out

# Run processes
if __name__ == '__main__':
    outq = Queue()
    spider=Spider()
    out = spider.run(outq)
    print("done")
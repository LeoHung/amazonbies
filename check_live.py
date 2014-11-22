from urllib import urlopen
import sys 

task_uri = [("q1", "q1?key=20630300497055296189489132603428150008912572451445788755351067609550255501160184017902946173672156459"),
        ("q2", "q2?userid=1473664038&tweet_time=2014-04-08+02:43:27"),
        ("q3", "q3?userid=2495192362"),
        ("q4", "q4?location=Hoopeston&date=2014-04-27&m=1&n=10"),
        ("q5", "q5?m=12&n=13"),
        ("q6", "q6?m=0&n=99")
]

for task, uri in task_uri:
    url = "http://ec2-54-174-82-118.compute-1.amazonaws.com/" + uri
    try:
        f = urlopen(url) 
        text = f.read()
        print text
    except:
        print "Error: %s fails" %(task)
        sys.exit(0)

print "OK"

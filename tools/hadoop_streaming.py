import os
import sys
import getopt
import subprocess

def getResults(cmd):
    p = subprocess.Popen(cmd, stderr = subprocess.PIPE, stdout = subprocess.PIPE, shell = True)
    p.wait()
    return p.stdout.read()

os.system('export LC_ALL=\"C\"')
os.system('export LC_COLLATE=\"C\"')
os.system('export LC_CTYPE=\"C\"')

python_script = ''
input_files = ''
output_folder = ''
mode = 'hadoop'
extra = ''

try:
    opts, args = getopt.getopt(sys.argv[1:], "hs:j:i:o:m:", ["help", "script=", "streaming-jar=", "input=", "output=", "mode=", "extra="])
    for k, v in opts:
        if k in ("-s", "--script"):
            python_script = v
        elif k in ("-j", "--streaming-jar"):
            hadoop_stream_jar = v
        elif k in ("-h", "--help"):
            print '--script \n --streaming-jar \n --input \n --output \n --mode \n'
        elif k in ("-i", "--input"):
            input_files = v
        elif k in ("-o", "--output"):
            output_folder = v
        elif k in ("-m", "--mode"):
            mode = v
        elif k in ("--extra"):
            extra = v

except getopt.GetoptError:
    print 'input parameters error'
    exit(1)

if len(python_script) == 0:
    print 'please specify the python script file'
    exit(1)

if mode == 'hadoop':
    if len(input_files) == 0:
        print 'please specify the input path'
        exit(1)

    if len(output_folder) == 0:
        print 'please specify the output path'
        exit(1)
    if output_folder.find('/t2/') > 0:
        print 'output path can not be in t2'
        exit(1)
    os.system('hadoop dfs -rmr ' + output_folder)
    root_path = os.path.dirname(os.path.abspath(__file__))
    
    hadoop_stream_jar = root_path + '/hadoop-streaming.jar'
    cmd = 'hadoop jar ' + hadoop_stream_jar + ' -mapper \'python ' + python_script + ' map\' --reducer \'python ' + python_script + ' reduce\' -file ' \
        + python_script + ' -file ' + root_path + '/mapreduce.py -input ' + input_files + ' -output ' + output_folder

    cmd += ' -jobconf mapred.job.name=reco.' + python_script
    cmd += ' ' + extra
    print cmd
    subP = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    subP.wait()

elif mode == 'local':
    cmd = 'python ' + python_script + ' map | sort | python ' + python_script + ' reduce'
    
    if len(output_folder) > 0:
        cmd += ' > ' + output_folder
    if len(input_files) > 0:
        cmd = 'cat ' + input_files + ' | ' + cmd
    subP = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
    subP.wait()
    for line in subP.stdout:
        print line.strip()
    for line in subP.stderr:
        print line.strip()

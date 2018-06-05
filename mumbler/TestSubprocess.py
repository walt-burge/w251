import subprocess


def forward_call(forward_node_id, first_word, max_words):
    ssh = subprocess.Popen(["ssh", forward_node_id, "mumbler "+first_word+" "+str(max_words)],
                           shell=False,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    ssh_result = ssh.stdout.readlines()

    return ssh_result


def ssh_call(forward_node_id, command):

    local_result = None

    ssh = subprocess.Popen(["ssh", forward_node_id, command],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    ssh_result, ssh_err = ssh.communicate(command)

    print "stdout: "+ssh_result
    print "stderr: "+ssh_err

    return ssh, ssh_result


if __name__ == "__main__":

    ssh, ssh_result = ssh_call("root@50.97.227.90", 'uname -a > tmp.txt; uname -a; exit')

    print "Result: " + str(ssh_result)
    ssh.kill()

    test = True


storm-example: word counter

在/src/main/resources/目录下创建words.txt文件，一个单词一行，然后用下面的命令运行这个拓扑:
mvn exec:java -Dexec.mainClass="MyTopology" -Dexec.args="src/main/resources/words.txt"
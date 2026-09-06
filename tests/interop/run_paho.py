#!/usr/bin/env python3
"""跑 Eclipse Paho interoperability 全部测试 (绕过 client_test.py 的 argv 限制)。

用法: python3 run_paho.py <interop目录> <port>
"""
import sys
import unittest

interop_dir = sys.argv[1]
port = int(sys.argv[2])
sys.path.insert(0, interop_dir)

import client_test  # noqa: E402

client_test.host = "127.0.0.1"
client_test.port = port
client_test.topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
client_test.wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
client_test.nosubscribe_topics = ("test/nosubscribe",)

unittest.main(module=client_test, argv=["paho-interop"])

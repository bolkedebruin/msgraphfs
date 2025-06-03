files = {
    "test/accounts.1.json": (
        b'{"amount": 100, "name": "Alice"}\n'
        b'{"amount": 200, "name": "Bob"}\n'
        b'{"amount": 300, "name": "Charlie"}\n'
        b'{"amount": 400, "name": "Dennis"}\n'
    ),
    "test/accounts.2.json": (
        b'{"amount": 500, "name": "Alice"}\n'
        b'{"amount": 600, "name": "Bob"}\n'
        b'{"amount": 700, "name": "Charlie"}\n'
        b'{"amount": 800, "name": "Dennis"}\n'
    ),
}

csv_files = {
    "csv/2014-01-01.csv": (b"name,amount,id\nAlice,100,1\nBob,200,2\nCharlie,300,3\n"),
    "csv/2014-01-02.csv": (b"name,amount,id\n"),
    "csv/2014-01-03.csv": (b"name,amount,id\nDennis,400,4\nEdith,500,5\nFrank,600,6\n"),
}
text_files = {
    "nested/file1": b"hello\n",
    "nested/file2": b"world",
    "nested/nested2/file1": b"hello\n",
    "nested/nested2/file2": b"world",
}
glob_files = {"file.dat": b"", "filexdat": b""}

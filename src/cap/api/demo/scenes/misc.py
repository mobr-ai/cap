# cap/api/demo/scenes/misc.py
MISC_SCENES = {
    "latest_5_blocks": {
        "match": "list the latest 5 blocks",
        "kv": {
            "result_type": "table",
            "data": {
                "values": [
                    {"key": "blockNumber", "values": ["5979789", "5979788", "5979787", "5979786", "5979785"]},
                    {"key": "slotNumber", "values": ["34724642", "34724623", "34724618", "34724610", "34724609"]},
                    {"key": "epochNumber", "values": ["80", "80", "80", "80", "80"]},
                    {
                        "key": "timestamp",
                        "values": [
                            "2021-07-14T19:28:53Z",
                            "2021-07-14T19:28:34Z",
                            "2021-07-14T19:28:29Z",
                            "2021-07-14T19:28:21Z",
                            "2021-07-14T19:28:20Z",
                        ],
                    },
                    {"key": "blockSize", "values": ["2518", "1197", "573", "3", "18899"]},
                    {
                        "key": "hash",
                        "values": [
                            "7ebff2ab745f908ff0d06cea3830c1b1c6020045c4a751e1f223e9a091fd881b",
                            "7b607fdbc570eb0375d3928a945b92bf25fa7ff38cc14a4545c2086238431f47",
                            "565607225351bdf21376f0a4f9cbbce90ca953102778f96c69549a9f985053d4",
                            "490c064b83d37fbc648db61bac223fdad37f1831f01b90b49fda0359c0aad543",
                            "b223bdabc24fa85b94c33db9c6ac382f022ff06a40851da0ced5c1a430301854",
                        ],
                    },
                    {"key": "transactionCount", "values": ["7", "1", "2", "0", "27"]},
                ]
            },
            "metadata": {
                "count": 5,
                "columns": [
                    "blockNumber",
                    "slotNumber",
                    "epochNumber",
                    "timestamp",
                    "blockSize",
                    "hash",
                    "transactionCount",
                ],
            },
        },
        "assistant_text": (
            "Here are the latest 5 blocks. In a production deployment, this table would be "
            "generated from on-chain block headers and enriched with derived fields."
        ),
    },

    "last_5_proposals": {
        "match": "show the last 5 proposals",
        "kv": {
            "result_type": "table",
            "data": {
                "values": [
                    {
                        "key": "proposalTxHash",
                        "values": [
                            "f8393f1ff814d3d52336a97712361fed933d9ef9e8d0909e1d31536a549fd22f",
                            "d16dffbae9d86a73cb343506e6712d79c278096dc25e8ba6900eb24522726bba",
                            "8f54d021c6e6fcdd5a4908f10a7b092fa31cd94db2e809f2e06d7ffa4d78773d",
                            "3285b7fd0da16d21e0b8f8910c37f77e17a57cfff8f513df4baf692954801088",
                            "03f671791fd97011f30e4d6b76c9a91f4f6bcfb60ee37e5399b9545bb3f2757a",
                        ],
                    },
                    {
                        "key": "proposalUrl",
                        "values": [
                            "<a href=\"https://ipfs.io/ipfs/bafkreiecqskxkmakkrzrs2xs2olh5jcwbuz5qr5gesp6merwcaydcaojiq\" target=\"_blank\">ipfs://bafkreiecqskxkmakkrzrs2xs2olh5jcwbuz5qr5gesp6merwcaydcaojiq</a>",
                            "<a href=\"https://ipfs.io/ipfs/Qmeme8EWugVPQeVghpqB53nvG5U4VT9zy3Ta545fEJPnqL\" target=\"_blank\">ipfs://Qmeme8EWugVPQeVghpqB53nvG5U4VT9zy3Ta545fEJPnqL</a>",
                            "<a href=\"https://ipfs.io/ipfs/bafkreicbxui5lbdrgcpjwhlti3rqkxfnd3vveiinkcu2zak5bny435w4yq\" target=\"_blank\">ipfs://bafkreicbxui5lbdrgcpjwhlti3rqkxfnd3vveiinkcu2zak5bny435w4yq</a>",
                            "<a href=\"https://most-brass-sun.quicknode-ipfs.com/ipfs/QmR7khTUdWyQFdNvyXDsuyZLUNsdfm7Ejo9wKfKdRE3ReG\" target=\"_blank\">https://most-brass-sun.quicknode-ipfs.com/ipfs/QmR7khTUdWyQFdNvyXDsuyZLUNsdfm7Ejo9wKfKdRE3ReG</a>",
                            "<a href=\"https://ipfs.io/ipfs/bafkreidl43ghacdpczaims63glq5kepaa63d63cr5mrpznv56jdm7e2eny\" target=\"_blank\">ipfs://bafkreidl43ghacdpczaims63glq5kepaa63d63cr5mrpznv56jdm7e2eny</a>",
                        ],
                    },
                    {"key": "voteCount", "values": ["27", "144", "276", "217", "226"]},
                    {"key": "yesCount", "values": ["26", "140", "259", "186", "160"]},
                    {"key": "noCount", "values": ["1", "4", "10", "16", "42"]},
                    {"key": "abstainCount", "values": ["0", "0", "7", "15", "24"]},
                    {
                        "key": "proposalTimestamp",
                        "values": [
                            "2025-12-08T22:34:44Z",
                            "2025-11-30T20:13:21Z",
                            "2025-11-27T19:50:18Z",
                            "2025-10-24T07:07:56Z",
                            "2025-10-23T15:59:15Z",
                        ],
                    },
                ]
            },
            "metadata": {
                "count": 5,
                "columns": [
                    "proposalTxHash",
                    "proposalUrl",
                    "voteCount",
                    "yesCount",
                    "noCount",
                    "abstainCount",
                    "proposalTimestamp",
                ],
            },
        },
        "assistant_text": "Here are the last 5 governance proposals (demo dataset) including their IPFS URLs.",
    },

    "markdown_only_formatting": {
        "match": "markdown formatting test",
        "break_sse_mode": "concat_payload",
        "assistant_text": (
            "# Markdown formatting smoke test\n\n"
            "This response has **no kv artifact** — it is *pure markdown text*.\n\n"
            "## Links\n"
            "- External: https://cardano.org\n"
            "- Explorer example: https://cardanoscan.io/\n\n"
            "## Lists\n"
            "1. Ordered item one\n"
            "2. Ordered item two\n"
            "   - Nested bullet A\n"
            "   - Nested bullet B\n\n"
            "## Code block\n"
            "```bash\n"
            "curl -s http://localhost:8000/api/v1/demo/nl/query \\\n"
            "  -H \"Authorization: Bearer <TOKEN>\" \\\n"
            "  -H \"Content-Type: application/json\" \\\n"
            "  -d '{\"query\":\"markdown formatting test\"}'\n"
            "```\n\n"
            "Inline code: `SELECT 1;`\n\n"
            "## Math\n"
            "Inline: $E=mc^2$  \n"
            "Display:\n"
            "$$\n"
            "\\sum_{i=1}^{n} i = \\frac{n(n+1)}{2}\n"
            "$$\n\n"
            "## Table\n"
            "| Metric | Value |\n"
            "| --- | ---: |\n"
            "| blocks | 5 |\n"
            "| txs | 27 |\n\n"
            "> Blockquote: this should render as a quote.\n"
        ),
    },
}

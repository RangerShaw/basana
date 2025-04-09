DEFAULTS = {
    "api": {
        "http": {
            "base_url": "",
            "timeout": 30,
        },
        "websockets": {
            "base_url": "",
            "heartbeat": 30,
            "spot": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
            "cross_margin": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
            "isolated_margin": {
                "user_data_stream": {
                    "heartbeat": 15 * 60,
                },
            },
        }
    }
}

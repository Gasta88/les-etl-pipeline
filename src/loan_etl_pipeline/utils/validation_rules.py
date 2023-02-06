import datetime

TO_DATE = lambda s: datetime.datetime.strptime(s, "%Y-%m-%d")
TO_NUMBER = lambda n: float(n)


def asset_schema():
    """
    Return validation schema for ASSETS data type.
    """
    schema = {
        "AS1": {
            "type": "datetime",
            "coerce": TO_DATE,
            "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime(2030, 12, 31),
        },
        "AS2": {
            "type": "string",
        },
        "AS3": {
            "type": "string",
        },
        "AS4": {
            "type": "string",
        },
        "AS5": {
            "type": "string",
        },
        "AS6": {
            "type": "string",
        },
        "AS7": {
            "type": "string",
        },
        "AS8": {
            "type": "string",
        },
        "AS15": {
            "type": "string",
        },
        "AS16": {
            "type": "string",
        },
        "AS17": {
            "type": "string",
            "nullable": True,
        },
        "AS18": {"type": "string", "allowed": [1, 2, 3, 4, 5], "nullable": True},
        "AS19": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime(2030, 12, 31),
            "nullable": True,
        },
        "AS20": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS22": {"type": "string", "allowed": [1, 2, 3, 4], "nullable": True},
        "AS23": {
            "type": "string",
            "allowed": ["Y", "N"],
        },
        "AS24": {"type": "string", "nullable": True},
        "AS25": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6, 7, 8],
            "nullable": True,
        },
        "AS26": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5],
            "nullable": True,
        },
        "AS27": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS28": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS29": {
            "type": "string",
            "allowed": ["Y", "N"],
            "nullable": True,
        },
        "AS30": {
            "type": "number",
            "nullable": True,
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS31": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS31": {
            "type": "string",
            "nullable": True,
        },
        "AS32": {
            "type": "string",
            "nullable": True,
        },
        "AS33": {
            "type": "string",
            "nullable": True,
        },
        "AS34": {
            "type": "string",
            "nullable": True,
        },
        "AS35": {
            "type": "string",
            "nullable": True,
        },
        "AS36": {
            "type": "string",
            "nullable": True,
        },
        "AS37": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS38": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS39": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS40": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS41": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS42": {
            "type": "string",
        },
        "AS43": {"type": "string", "nullable": True},
        "AS44": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS45": {
            "type": "string",
            "nullable": True,
        },
        "AS50": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS51": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS52": {
            "type": "string",
        },
        "AS53": {"type": "string", "allowed": ["Y", "N"]},
        "AS54": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS55": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS56": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS57": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
            "nullable": True,
        },
        "AS58": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6],
            "nullable": True,
        },
        "AS59": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6],
            "nullable": True,
        },
        "AS60": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS61": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS62": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6, 7],
        },
        "AS63": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS64": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS65": {
            "type": "string",
            "allowed": [1, 2, 3],
            "nullable": True,
        },
        "AS66": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS67": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS68": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5],
            "nullable": True,
        },
        "AS69": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS70": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS71": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS80": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS81": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS82": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS83": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 56, 7, 8, 9, 10],
        },
        "AS84": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 56, 7, 8, 9, 10, 11, 12],
        },
        "AS85": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS86": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS87": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS88": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS89": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS90": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS91": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS92": {
            "type": "string",
            "nullable": True,
        },
        "AS93": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS94": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6],
            "nullable": True,
        },
        "AS100": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS101": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS102": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS103": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS104": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS105": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS106": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS107": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS108": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS109": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS110": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS111": {
            "type": "string",
            "nullable": True,
        },
        "AS112": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS115": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS116": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS117": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS118": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS119": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS120": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS121": {"type": "string", "allowed": ["Y", "N"]},
        "AS122": {"type": "string", "allowed": ["Y", "N"]},
        "AS123": {
            "type": "string",
            "allowed": [1, 2, 3, 4],
            "nullable": True,
        },
        "AS124": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS125": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS126": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS127": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS128": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS129": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6],
            "nullable": True,
        },
        "AS130": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS131": {"type": "string", "allowed": ["Y", "N"], "nullable": True},
        "AS132": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "AS133": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "AS134": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS135": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS136": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "AS137": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "AS138": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
    }

    return schema


def collateral_schema():
    """
    Return validation schema for COLLATERALS data type.
    """
    schema = {
        "CS1": {
            "type": "string",
        },
        "CS2": {
            "type": "string",
        },
        "CS3": {"type": "string", "allowed": [1, 2, 3, 4, 5], "nullable": True},
        "CS4": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "CS5": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "CS6": {
            "type": "string",
            "allowed": [
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
                23,
            ],
            "nullable": True,
        },
        "CS7": {"type": "string", "allowed": ["Y", "N"], "nullable": True},
        "CS8": {"type": "string", "allowed": ["Y", "N"], "nullable": True},
        "CS9": {"type": "string", "allowed": ["Y", "N"], "nullable": True},
        "CS10": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "CS11": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "CS12": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "CS13": {
            "type": "string",
            "allowed": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "nullable": True,
        },
        "CS214": {
            "type": "string",
            "allowed": [1, 2, 3],
            "nullable": True,
        },
        "CS15": {
            "type": "string",
            "allowed": [1, 2, 3, 4],
        },
        "CS16": {
            "type": "string",
        },
        "CS17": {"type": "string", "nullable": True},
        "CS18": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "CS19": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "CS20": {"type": "string", "nullable": True},
        "CS21": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "CS22": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
            "nullable": True,
        },
        "CS23": {"type": "string", "allowed": [1, 2, 3, 4], "nullable": True},
        "CS24": {
            "type": "string",
        },
        "CS25": {"type": "string", "nullable": True},
        "CS26": {"type": "string", "nullable": True},
        "CS27": {"type": "string", "nullable": True},
        "CS28": {"type": "number", "coerce": TO_NUMBER, "min": 0.0},
    }
    return schema


def bond_info_schema():
    """
    Return validation schema for BOND_INFO data type.
    """
    schema = {
        "BS1": {
            "type": "datetime",
            "coerce": TO_DATE,
            # "min": datetime.datetime(2012, 1, 1),
            "max": datetime.datetime.now(),
        },
        "BS2": {
            "type": "string",
        },
        "BS3": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "BS4": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "BS5": {
            "type": "string",
            "allowed": ["Y", "N"],
        },
        "BS6": {
            "type": "string",
            "nullable": True,
        },
        "BS11": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
            "nullable": True,
        },
        "BS12": {
            "type": "string",
            "allowed": ["Y", "N"],
        },
        "BS13": {
            "type": "number",
            "coerce": TO_NUMBER,
            "min": 0.0,
        },
        "BS19": {"type": "string"},
        "BS20": {"type": "string"},
        "BS30": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS31": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS33": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS34": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS35": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS36": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
        "BS37": {"type": "number", "coerce": TO_NUMBER, "min": 0.0, "nullable": True},
    }
    return schema


def amortisation_schema():
    """
    Return validation schema for AMORTISATION PROFILE data type.
    """
    schema = {"AS3": {"type": "string"}}
    for i in range(150, 390):
        if i % 2 == 0:
            schema[f"AS{i}"] = {"type": "number", "coerce": TO_NUMBER, "min": 0.0}
        else:
            schema[f"AS{i}"] = {
                "type": "datetime",
                "coerce": TO_DATE,
                "min": datetime.datetime(2012, 1, 1),
                "max": (datetime.datetime.now() + datetime.timedelta(years=6)),
            }
    for i in range(390, 1350):
        if i % 2 == 0:
            schema[f"AS{i}"] = {
                "type": "number",
                "coerce": TO_NUMBER,
                "min": 0.0,
                "nullable": True,
            }
        else:
            schema[f"AS{i}"] = {
                "type": "datetime",
                "coerce": TO_DATE,
                "min": datetime.datetime(2012, 1, 1),
                "max": (datetime.datetime.now() + datetime.timedelta(years=6)),
                "nullable": True,
            }
    return schema

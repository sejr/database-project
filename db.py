import csv, uuid, sqlite3, random, datetime

class Library:
    def __init__(self):
        self.artists, self.albums, self.tracks, self.reviews = {}, {}, {}, {}
        self.physical_copies, self.digital_copies = {}, {}
        self.patrons, self.patron_cards = {}, {}
        self.branches, self.checkouts = {}, {}
        self.db = sqlite3.connect("library.db")
        self.cursor = self.db.cursor()
        self.initialize_schema()

    def initialize_schema(self):
        """
        Setting up the foundation for the library database. We start by clearing
        all existing tables so that we can run this script repeatedly (sqlite
        would otherwise persist all of our information).
        """
        self.cursor.execute("DROP TABLE IF EXISTS BRANCH")
        self.cursor.execute("DROP TABLE IF EXISTS PATRON")
        self.cursor.execute("DROP TABLE IF EXISTS PATRON_CARD")
        self.cursor.execute("DROP TABLE IF EXISTS PHYSICAL_COPY")
        self.cursor.execute("DROP TABLE IF EXISTS CHECKOUT")

        self.cursor.execute("DROP TABLE IF EXISTS ARTIST")
        self.cursor.execute("DROP TABLE IF EXISTS ALBUM")
        self.cursor.execute("DROP TABLE IF EXISTS TRACK")

        self.cursor.execute("DROP TABLE IF EXISTS REVIEW")

        self.cursor.execute("""
            CREATE TABLE `BRANCH` (
                `id`        VARCHAR(255) NOT NULL UNIQUE,
                `address`   VARCHAR(255) NOT NULL UNIQUE,
                `name`      VARCHAR(255) NOT NULL,

                PRIMARY KEY (`id`)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `ARTIST` (

                `id`    VARCHAR(255) NOT NULL UNIQUE,
                `name`  VARCHAR(255) NOT NULL,

                PRIMARY KEY (`id`)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `ALBUM` (

                `id`            VARCHAR(255) NOT NULL UNIQUE,
                `artist_id`     VARCHAR(255) NOT NULL,
                `title`         VARCHAR(255) NOT NULL,
                `genre`         VARCHAR(255) NOT NULL,
                `release_year`  INT          NOT NULL,

                PRIMARY KEY (`id`),
                FOREIGN KEY (`artist_id`) REFERENCES ARTIST(id)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `TRACK` (

                `id`                VARCHAR(255) NOT NULL UNIQUE,
                `title`             VARCHAR(255) NOT NULL,
                `album_id`          VARCHAR(255) NOT NULL,
                `length_seconds`    INT          NOT NULL,
                `size_bytes`        INT          NOT NULL,

                PRIMARY KEY (`id`)
                FOREIGN KEY (`album_id`) REFERENCES ALBUM(id)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `PATRON` (

                `id`         VARCHAR(255) NOT NULL UNIQUE,
                `email`      VARCHAR(255) NOT NULL UNIQUE,
                `name_first` VARCHAR(255),
                `name_last`  VARCHAR(255),
                `phone`      INT,

                PRIMARY KEY (`id`)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `PATRON_CARD` (
                `card_number`   VARCHAR(255) NOT NULL UNIQUE,
                `patron_id`     VARCHAR(255) NOT NULL,
                `created`       DATETIME     NOT NULL,

                PRIMARY KEY (`card_number`)
                FOREIGN KEY (`patron_id`) REFERENCES PATRON(id)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `PHYSICAL_COPY` (

                `id`                VARCHAR(255) NOT NULL UNIQUE,
                `album_id`          VARCHAR(255) NOT NULL,
                `branch_id`         VARCHAR(255) NOT NULL,
                `checkout_count`    INT NOT NULL DEFAULT '0',
                `available_count`   INT NOT NULL,

                PRIMARY KEY (`id`,`album_id`,`branch_id`)
                FOREIGN KEY (`album_id`) REFERENCES ALBUM(`id`)
                FOREIGN KEY (`branch_id`) REFERENCES BRANCH(`id`)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `REVIEW` (

                `id`        VARCHAR(255) NOT NULL UNIQUE,
                `author_id` VARCHAR(255) NOT NULL,
                `album_id`  VARCHAR(255) NOT NULL,
                `datetime`  DATETIME     NOT NULL,

                `score`     INT DEFAULT '5',
                `comments`  TEXT,

                PRIMARY KEY (`id`)
                FOREIGN KEY (`author_id`) REFERENCES PATRON(id)
                FOREIGN KEY (`album_id`)  REFERENCES ALBUM(id)
            );
            """)
        self.cursor.execute("""
            CREATE TABLE `CHECKOUT` (
                `id`        VARCHAR(255) NOT NULL UNIQUE,
                `card_used` VARCHAR(255) NOT NULL,
                `branch_id` VARCHAR(255) NOT NULL,
                `album_id`  VARCHAR(255) NOT NULL,
                `datetime`  DATETIME NOT NULL,

                PRIMARY KEY (`id`,`card_used`,`datetime`)
                FOREIGN KEY (`card_used`) REFERENCES PATRON_CARD(`card_number`)
                FOREIGN KEY (`branch_id`) REFERENCES BRANCH(`id`)
                FOREIGN KEY (`album_id`)  REFERENCES ALBUM(`id`)
            );
            """)
        self.db.commit()

    def add_patron(self, patron):
        """
        If the artist is already in our cache, we do nothing. Otherwise, we add
        them to the cache and execute the appropriate SQL statement to insert
        them into our library database.
        """
        patron["id"] = str(uuid.uuid5(
            uuid.NAMESPACE_DNS, patron["name_first"] + patron["name_last"]))

        if patron["id"] in self.patrons:
            # print(patron["name_first"], patron["name_last"], "already has an account.")
            pass
        else:
            # Store the artist in our cache
            self.patrons[patron["id"]] = patron
            # Execute SQL query to add artist to appropriate relation
            self.cursor.execute("""
                INSERT INTO PATRON (id, email, name_first, name_last, phone)
                VALUES ("{}", "{}", "{}", "{}", "{}");
                """.format(patron["id"], patron["email"], patron["name_first"],
                    patron["name_last"], patron["phone"]))
            self.db.commit()

    def add_artist(self, artist):
        """
        If the artist is already in our cache, we do nothing. Otherwise, we add
        them to the cache and execute the appropriate SQL statement to insert
        them into our library database.
        """
        if artist["id"] in self.artists:
            # print(artist["name"] + " already exists in the library.")
            pass
        else:
            # Store the artist in our cache
            self.artists[artist["id"]] = artist
            # Execute SQL query to add artist to appropriate relation
            self.cursor.execute("INSERT INTO ARTIST (id, name) VALUES ('" +
                artist["id"] + "', '" + artist["name"] + "');")
            self.db.commit()

    def add_album(self, album):
        """
        If the album is already in our cache, we do nothing. Otherwise, we add
        it to the cache and execute the appropriate SQL statement to insert
        it into our library database.
        """
        if album["id"] in self.albums:
            # print(album["title"] + " already exists in the library.")
            pass
        else:
            # Store the artist in our cache
            self.albums[album["id"]] = album
            # Execute SQL query to add artist to appropriate relation
            self.cursor.execute("""
                INSERT INTO ALBUM (id, artist_id, title, genre, release_year)
                VALUES ("{}", "{}", "{}", "{}", "{}");
                """.format(album["id"], album["artist_id"], album["title"],
                    album["genre"], album["release_year"]))
            self.db.commit()

    def add_track(self, track):
        """
        If the track is already in our cache, we do nothing. Otherwise, we add
        it to the cache and execute the appropriate SQL statement to insert
        it into our library database.
        """
        if track["id"] in self.tracks:
            # print(track["title"] + " already exists in the library.")
            pass
        else:
            # Store the artist in our cache
            self.tracks[track["id"]] = track
            # Execute SQL query to add artist to appropriate relation
            self.cursor.execute("""
                INSERT INTO TRACK (id, title, album_id, length_seconds, size_bytes)
                VALUES ("{}", "{}", "{}", "{}", "{}");
                """.format(track["id"], track["title"], 
                    track["album_id"], track["length_seconds"], track["size_bytes"]))
            self.db.commit()

    def parse_entry(self, entry):
        """
        Handles the bulk of the work when it comes to adding new information
        to the database. Specifically we convert the entry dictionaries into
        those that align with our database schema. We then pass the appropriate
        information off to insert new data into our tables.
        """
        artist, album, track = {}, {}, {}

        artist["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, entry["artist_name"]))
        artist["name"] = entry["artist_name"]
        self.add_artist(artist)

        album["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, entry["album_title"]))
        album["release_year"] = entry["release_year"]
        album["title"] = entry["album_title"]
        album["artist_id"] = artist["id"]
        album["genre"] = entry["genre"]
        self.add_album(album)

        track["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, entry["artist_name"] + entry["track"]))
        track["length_seconds"] = entry["length_seconds"]
        track["size_bytes"] = entry["size_bytes"]
        track["album_id"] = album["id"]
        track["title"] = entry["track"]
        self.add_track(track)

    def generate_reviews(self):
        for i in range(100):
            review = {}
            review["id"] = str(uuid.uuid4())
            review["author_id"] = random.choice(self.patrons.keys())
            review["album_id"] = random.choice(self.albums.keys())
            review["datetime"] = str(datetime.datetime.now())
            review["score"] = random.randint(1, 5)
            review["comments"] = "This is an automatically generated review."
            self.cursor.execute("""
                INSERT INTO REVIEW (id, author_id, album_id, `datetime`, score, comments)
                VALUES ("{}", "{}", "{}", "{}", {}, "{}");
                """.format(review["id"], review["author_id"], review["album_id"],
                    review["datetime"], review["score"], review["comments"]))
        self.db.commit()

    def generate_catalog(self):
        for i in range(100):
            copy = {}
            album = random.choice(self.albums.keys())
            branch = random.choice(self.branches.keys())
            copy["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(album) + str(branch)))
            if copy["id"] in self.physical_copies:
                pass
            else:
                copy["album_id"] = album
                copy["branch_id"] = branch
                copy["checkout_count"] = random.randint(1, 100)
                copy["available_count"] = random.randint(1, 10)
                self.physical_copies[copy["id"]] = copy
                self.cursor.execute("""
                    INSERT INTO PHYSICAL_COPY (id, album_id, branch_id, checkout_count, available_count)
                    VALUES ("{}", "{}", "{}", "{}", "{}")
                    """.format(copy["id"], copy["album_id"], copy["branch_id"], 
                        copy["checkout_count"], copy["available_count"]))
        self.db.commit()

    def generate_cards(self):
        for patron in self.patrons:
            card = {}
            card["card_number"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(self.patrons[patron]["id"])))
            card["patron_id"] = str(self.patrons[patron]["id"])
            card["created"] = str(datetime.datetime.now())
            self.patron_cards[card["card_number"]] = card
            self.cursor.execute("""
                INSERT INTO PATRON_CARD (card_number, patron_id, created)
                VALUES ("{}", "{}", "{}")
                """.format(card["card_number"], card["patron_id"], card["created"]))
        self.db.commit()

    def generate_checkouts(self):
        for i in range(100):
            checkout = {}
            checkout["id"] = uuid.uuid4()
            checkout["card_used"] = random.choice(self.patron_cards.keys())
            checkout["branch_id"] = random.choice(self.branches.keys())
            checkout["album_id"] = random.choice(self.albums.keys())
            checkout["datetime"] = str(datetime.datetime.now())
            self.checkouts[checkout["id"]] = checkout
            self.cursor.execute("""
                INSERT INTO CHECKOUT (id, card_used, branch_id, album_id, `datetime`)
                VALUES ("{}", "{}", "{}", "{}", "{}")
                """.format(checkout["id"], checkout["card_used"], checkout["branch_id"],
                    checkout["album_id"], checkout["datetime"]))
        self.db.commit()


if __name__ == "__main__":
    lib = Library()

    branch = {}
    branch["id"] = uuid.uuid4()
    branch["address"] = "1234 My Test Address, Columbus OH, 43201"
    branch["name"] = "Mock Library Branch"
    lib.branches[branch["id"]] = branch
    lib.cursor.execute("""
        INSERT INTO BRANCH (id, address, name)
        VALUES ("{}", "{}", "{}")
        """.format(branch["id"], branch["address"], branch["name"]))

    # Taking the test data and adding it to the library database.
    with open("mock_catalog.csv") as catalog:
        reader = csv.DictReader(catalog)
        for row in reader:
            lib.parse_entry(row)

    with open("mock_patrons.csv") as patrons:
        reader = csv.DictReader(patrons)
        for row in reader:
            lib.add_patron(row)

    lib.generate_reviews()
    lib.generate_catalog()
    lib.generate_cards()
    lib.generate_checkouts()

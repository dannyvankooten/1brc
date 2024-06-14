#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

char *city_names[] = {
  "Abha",
  "Abidjan",
  "Abéché",
  "Accra",
  "Addis Ababa",
  "Adelaide",
  "Aden",
  "Ahvaz",
  "Albuquerque",
  "Alexandra",
  "Alexandria",
  "Algiers",
  "Alice Springs",
  "Almaty",
  "Amsterdam",
  "Anadyr",
  "Anchorage",
  "Andorra la Vella",
  "Ankara",
  "Antananarivo",
  "Antsiranana",
  "Arkhangelsk",
  "Ashgabat",
  "Asmara",
  "Assab",
  "Astana",
  "Athens",
  "Atlanta",
  "Auckland",
  "Austin",
  "Baghdad",
  "Baguio",
  "Baku",
  "Baltimore",
  "Bamako",
  "Bangkok",
  "Bangui",
  "Banjul",
  "Barcelona",
  "Bata",
  "Batumi",
  "Beijing",
  "Beirut",
  "Belgrade",
  "Belize City",
  "Benghazi",
  "Bergen",
  "Berlin",
  "Bilbao",
  "Birao",
  "Bishkek",
  "Bissau",
  "Blantyre",
  "Bloemfontein",
  "Boise",
  "Bordeaux",
  "Bosaso",
  "Boston",
  "Bouaké",
  "Bratislava",
  "Brazzaville",
  "Bridgetown",
  "Brisbane",
  "Brussels",
  "Bucharest",
  "Budapest",
  "Bujumbura",
  "Bulawayo",
  "Burnie",
  "Busan",
  "Cabo San Lucas",
  "Cairns",
  "Cairo",
  "Calgary",
  "Canberra",
  "Cape Town",
  "Changsha",
  "Charlotte",
  "Chiang Mai",
  "Chicago",
  "Chihuahua",
  "Chișinău",
  "Chittagong",
  "Chongqing",
  "Christchurch",
  "City of San Marino",
  "Colombo",
  "Columbus",
  "Conakry",
  "Copenhagen",
  "Cotonou",
  "Cracow",
  "Da Lat",
  "Da Nang",
  "Dakar",
  "Dallas",
  "Damascus",
  "Dampier",
  "Dar es Salaam",
  "Darwin",
  "Denpasar",
  "Denver",
  "Detroit",
  "Dhaka",
  "Dikson",
  "Dili",
  "Djibouti",
  "Dodoma",
  "Dolisie",
  "Douala",
  "Dubai",
  "Dublin",
  "Dunedin",
  "Durban",
  "Dushanbe",
  "Edinburgh",
  "Edmonton",
  "El Paso",
  "Entebbe",
  "Erbil",
  "Erzurum",
  "Fairbanks",
  "Fianarantsoa",
  "Flores,  Petén",
  "Frankfurt",
  "Fresno",
  "Fukuoka",
  "Gabès",
  "Gaborone",
  "Gagnoa",
  "Gangtok",
  "Garissa",
  "Garoua",
  "George Town",
  "Ghanzi",
  "Gjoa Haven",
  "Guadalajara",
  "Guangzhou",
  "Guatemala City",
  "Halifax",
  "Hamburg",
  "Hamilton",
  "Hanga Roa",
  "Hanoi",
  "Harare",
  "Harbin",
  "Hargeisa",
  "Hat Yai",
  "Havana",
  "Helsinki",
  "Heraklion",
  "Hiroshima",
  "Ho Chi Minh City",
  "Hobart",
  "Hong Kong",
  "Honiara",
  "Honolulu",
  "Houston",
  "Ifrane",
  "Indianapolis",
  "Iqaluit",
  "Irkutsk",
  "Istanbul",
  "İzmir",
  "Jacksonville",
  "Jakarta",
  "Jayapura",
  "Jerusalem",
  "Johannesburg",
  "Jos",
  "Juba",
  "Kabul",
  "Kampala",
  "Kandi",
  "Kankan",
  "Kano",
  "Kansas City",
  "Karachi",
  "Karonga",
  "Kathmandu",
  "Khartoum",
  "Kingston",
  "Kinshasa",
  "Kolkata",
  "Kuala Lumpur",
  "Kumasi",
  "Kunming",
  "Kuopio",
  "Kuwait City",
  "Kyiv",
  "Kyoto",
  "La Ceiba",
  "La Paz",
  "Lagos",
  "Lahore",
  "Lake Havasu City",
  "Lake Tekapo",
  "Las Palmas de Gran Canaria",
  "Las Vegas",
  "Launceston",
  "Lhasa",
  "Libreville",
  "Lisbon",
  "Livingstone",
  "Ljubljana",
  "Lodwar",
  "Lomé",
  "London",
  "Los Angeles",
  "Louisville",
  "Luanda",
  "Lubumbashi",
  "Lusaka",
  "Luxembourg City",
  "Lviv",
  "Lyon",
  "Madrid",
  "Mahajanga",
  "Makassar",
  "Makurdi",
  "Malabo",
  "Malé",
  "Managua",
  "Manama",
  "Mandalay",
  "Mango",
  "Manila",
  "Maputo",
  "Marrakesh",
  "Marseille",
  "Maun",
  "Medan",
  "Mek'ele",
  "Melbourne",
  "Memphis",
  "Mexicali",
  "Mexico City",
  "Miami",
  "Milan",
  "Milwaukee",
  "Minneapolis",
  "Minsk",
  "Mogadishu",
  "Mombasa",
  "Monaco",
  "Moncton",
  "Monterrey",
  "Montreal",
  "Moscow",
  "Mumbai",
  "Murmansk",
  "Muscat",
  "Mzuzu",
  "N'Djamena",
  "Naha",
  "Nairobi",
  "Nakhon Ratchasima",
  "Napier",
  "Napoli",
  "Nashville",
  "Nassau",
  "Ndola",
  "New Delhi",
  "New Orleans",
  "New York City",
  "Ngaoundéré",
  "Niamey",
  "Nicosia",
  "Niigata",
  "Nouadhibou",
  "Nouakchott",
  "Novosibirsk",
  "Nuuk",
  "Odesa",
  "Odienné",
  "Oklahoma City",
  "Omaha",
  "Oranjestad",
  "Oslo",
  "Ottawa",
  "Ouagadougou",
  "Ouahigouya",
  "Ouarzazate",
  "Oulu",
  "Palembang",
  "Palermo",
  "Palm Springs",
  "Palmerston North",
  "Panama City",
  "Parakou",
  "Paris",
  "Perth",
  "Petropavlovsk-Kamchatsky",
  "Philadelphia",
  "Phnom Penh",
  "Phoenix",
  "Pittsburgh",
  "Podgorica",
  "Pointe-Noire",
  "Pontianak",
  "Port Moresby",
  "Port Sudan",
  "Port Vila",
  "Port-Gentil",
  "Portland {OR}",
  "Porto",
  "Prague",
  "Praia",
  "Pretoria",
  "Pyongyang",
  "Rabat",
  "Rangpur",
  "Reggane",
  "Reykjavík",
  "Riga",
  "Riyadh",
  "Rome",
  "Roseau",
  "Rostov-on-Don",
  "Sacramento",
  "Saint Petersburg",
  "Saint-Pierre",
  "Salt Lake City",
  "San Antonio",
  "San Diego",
  "San Francisco",
  "San Jose",
  "San José",
  "San Juan",
  "San Salvador",
  "Sana'a",
  "Santo Domingo",
  "Sapporo",
  "Sarajevo",
  "Saskatoon",
  "Seattle",
  "Ségou",
  "Seoul",
  "Seville",
  "Shanghai",
  "Singapore",
  "Skopje",
  "Sochi",
  "Sofia",
  "Sokoto",
  "Split",
  "St. John's",
  "St. Louis",
  "Stockholm",
  "Surabaya",
  "Suva",
  "Suwałki",
  "Sydney",
  "Tabora",
  "Tabriz",
  "Taipei",
  "Tallinn",
  "Tamale",
  "Tamanrasset",
  "Tampa",
  "Tashkent",
  "Tauranga",
  "Tbilisi",
  "Tegucigalpa",
  "Tehran",
  "Tel Aviv",
  "Thessaloniki",
  "Thiès",
  "Tijuana",
  "Timbuktu",
  "Tirana",
  "Toamasina",
  "Tokyo",
  "Toliara",
  "Toluca",
  "Toronto",
  "Tripoli",
  "Tromsø",
  "Tucson",
  "Tunis",
  "Ulaanbaatar",
  "Upington",
  "Ürümqi",
  "Vaduz",
  "Valencia",
  "Valletta",
  "Vancouver",
  "Veracruz",
  "Vienna",
  "Vientiane",
  "Villahermosa",
  "Vilnius",
  "Virginia Beach",
  "Vladivostok",
  "Warsaw",
  "Washington, D.C.",
  "Wau",
  "Wellington",
  "Whitehorse",
  "Wichita",
  "Willemstad",
  "Winnipeg",
  "Wrocław",
  "Xi'an",
  "Yakutsk",
  "Yangon",
  "Yaoundé",
  "Yellowknife",
  "Yerevan",
  "Yinchuan",
  "Zagreb",
  "Zanzibar City",
  "Zürich",
};

uint64_t fnv1a(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h *= 0x811C9DC5;
    h ^= (unsigned char) *key;
    key++;
  }
  return h;
}

uint64_t djb2(char *key) {
  uint64_t h = 5381;
  while (*key != 0) {
    h <<= 5;
    h += h;
    h += (unsigned char) *key;
    key++;
  }
  return h;
}

uint64_t sdbm(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h = (unsigned char)*key + (h << 6) + (h << 16) - h;
    key++;
  }
  return h;
}

uint64_t m31(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h = (h * 31) + ((unsigned char)*key);
    key++;
  }
  return h;
}

uint64_t m31s(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h = (h * 31) + ((unsigned char)*key - 'A');
    key++;
  }
  return h;
}

uint64_t m32(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h = (h * 32) + ((unsigned char)*key - 'A');
    key++;
  }
  return h;
}

uint64_t mz(char *key) {
  uint64_t h = 0;
  while (*key != 0) {
    h = (h * 'z') + ((unsigned char) *key - 'A');
    key++;
  }
  return h;
}

uint64_t adler32(char *key) {
  uint64_t h1 = 0;
  uint64_t h2 = 0;
  while (*key != 0) {
    h1 = (h1 + (unsigned char)*key) % 65521u;
    h2 = (h2 + h1) % 65521u;
    key++;
  }
  return (h2 << 16) | h1;
}

uint64_t rshash(char* key)
{
  uint64_t hash = 0;

  while (*key != 0) {
    hash = (unsigned char)*key + (hash << 13) + (hash << 9) + (hash << 4) - hash;
    key++;
  }
  hash += (hash << 3);
  hash ^= (hash >> 11);
  hash += (hash << 15);
  return hash;
}


int main(void) {
  size_t ncities = sizeof(city_names) / sizeof(city_names[0]);

  struct hash_function {
    char *name;
    uint64_t (*function)(char *);
  } hash_functions[] = {
    {
      "fnv1a",
      &fnv1a,
    },
    {
      "djb2",
      &djb2,
    },
    {
      "m31",
      &m31,
    },
    {
      "m31s",
      &m31s,
    },
    {
      "m32",
      &m32,
    },
    {
      "adler32",
      &adler32,
    },
    {
      "mz",
      &mz,
    },
    {
      "rshash",
      &rshash,
    },
  };
  size_t nhash_functions = sizeof(hash_functions) / sizeof(hash_functions[0]);

  for (unsigned int f = 0; f < nhash_functions; f++) {
    printf("%s\n", hash_functions[f].name);
    printf("%s    %s       %s\n", "capacity", "%", "time");

    for (unsigned int cap = 512; cap < 512 << 6; cap <<= 1) {
      char *map = malloc(sizeof(char) * cap);
      memset(map, 0, cap * sizeof(char));
      int collisions = 0;
      clock_t start = clock();

      for (unsigned int c = 0; c < ncities; c++) {
        uint64_t slot = hash_functions[f].function(city_names[c]) % cap;
        if (map[slot] == 1) {
          collisions++;
        } else {
          map[slot] = 1;
        }
      }

      printf("%% %5d:    %.2f    %ld ns\n", cap, (double)collisions / (double)ncities, ((clock() - start) * 1000000 / CLOCKS_PER_SEC));
      free(map);
    }

    printf("\n");
  }
}

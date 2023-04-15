import shutil


_test_data = """Anarchism is a philosophy that embodies many diverse attitudes, tendencies and schools of thought; 
    as such, disagreement over questions of values, ideology and tactics is common. The compatibility of capitalism, 
    nationalism, and religion with anarchism is widely disputed. Similarly, anarchism enjoys complex relationships 
    with ideologies such as Marxism, communism, collectivism, syndicalism/trade unionism, and capitalism. Anarchists 
    may be motivated by humanism, divine authority, enlightened self-interest, veganism or any number of alternative 
    ethical doctrines.\n\nPhenomena such as civilisation, technology (e.g. within anarcho-primitivism), 
    and the democratic process may be sharply criticised within some anarchist tendencies and simultaneously lauded 
    in others.\n\nOn a tactical level, while propaganda of the deed was a tactic used by anarchists in the 19th 
    century (e.g. the Nihilist movement), some contemporary anarchists espouse alternative direct action methods such 
    as nonviolence, counter-economics and anti-state cryptography to bring about an anarchist society. About the 
    scope of an anarchist society, some anarchists advocate a global one, while others do so by local ones. The 
    diversity in anarchism has led to widely different use of identical terms among different anarchist traditions, 
    which has led to many definitional concerns in anarchist theory.\nIntersecting and overlapping between various 
    schools of thought, certain topics of interest and internal disputes have proven perennial within anarchist 
    theory.\n\nAn important current within anarchism is free love. Free love advocates sometimes traced their roots 
    back to Josiah Warren and to experimental communities, viewed sexual freedom as a clear, direct expression of an 
    individual\'s sovereignty. Free love particularly stressed women\'s rights since most sexual laws discriminated 
    against women: for example, marriage laws and anti-birth control measures. The most important American free love 
    journal was "Lucifer the Lightbearer" (1883–1907) edited by Moses Harman and Lois Waisbrooker, but also there 
    existed Ezra Heywood and Angela Heywood\'s "The Word" (1872–1890, 1892–1893). "Free Society" (1895–1897 as "The 
    Firebrand"; 1897–1904 as "Free Society") was a major anarchist newspaper in the United States at the end of the 
    19th and beginning of the 20th centuries. The publication advocated free love and women\'s rights, and critiqued 
    "Comstockery" – censorship of sexual information. Also M. E. Lazarus was an important American individualist 
    anarchist who promoted free love.\n\nIn New York City\'s Greenwich Village, bohemian feminists and socialists 
    advocated self-realisation and pleasure for women (and also men) in the here and now. They encouraged playing 
    with sexual roles and sexuality, and the openly bisexual radical Edna St. Vincent Millay and the lesbian 
    anarchist Margaret Anderson were prominent among them. Discussion groups organised by the Villagers were 
    frequented by Emma Goldman, among others. Magnus Hirschfeld noted in 1923 that Goldman "has campaigned boldly and 
    steadfastly for individual rights, and especially for those deprived of their rights. Thus it came about that she 
    was the first and only woman, indeed the first and only American, to take up the defence of homosexual love 
    before the general public." In fact, before Goldman, heterosexual anarchist Robert Reitzel (1849–1898) spoke 
    positively of homosexuality from the beginning of the 1890s in his Detroit-based German language journal "Der 
    arme Teufel" (English: The Poor Devil). In Argentina anarcha-feminist Virginia Bolten published the newspaper 
    called "" (English: The Woman\'s Voice), which was published nine times in Rosario between 8 January 1896 and 1 
    January 1897, and was revived, briefly, in 1901.\n\nIn Europe the main propagandist of free love within 
    individualist anarchism was Emile Armand. He proposed the concept of "la camaraderie amoureuse" to speak of free 
    love as the possibility of voluntary sexual encounter between consenting adults. He was also a consistent 
    proponent of polyamory. In Germany the stirnerists Adolf Brand and John Henry Mackay were pioneering campaigners 
    for the acceptance of male bisexuality and homosexuality. Mujeres Libres was an anarchist women\'s organisation 
    in Spain that aimed to empower working class women. It was founded in 1936 by Lucía Sánchez Saornil, 
    Mercedes Comaposada and Amparo Poch y Gascón and had approximately 30,000 members. The organisation was based on 
    the idea of a "double struggle" for women\'s liberation and social revolution and argued that the two objectives 
    were equally important and should be pursued in parallel. In order to gain mutual support, they created networks 
    of women anarchists. Lucía Sánchez Saornil was a main founder of the Spanish anarcha-feminist federation Mujeres 
    Libres who was open about her lesbianism. She was published in a variety of literary journals where working under 
    a male pen name, she was able to explore lesbian themes at a time when homosexuality was criminalised and subject 
    to censorship and punishment.\n\nMore recently, the British anarcho-pacifist Alex Comfort gained notoriety during 
    the sexual revolution for writing the bestseller sex manual "The Joy of Sex". The issue of free love has a 
    dedicated treatment in the work of French anarcho-hedonist philosopher Michel Onfray in such works as "Théorie du 
    corps amoureux : pour une érotique solaire" (2000) and "L\'invention du plaisir : fragments cyréaniques" (
    2002).\n\n For English anarchist William Godwin education was "the main means by which change would be achieved." 
    Godwin saw that the main goal of education should be the promotion of happiness. For Godwin education had to have 
    "A respect for the child\'s autonomy which precluded any form of coercion," "A pedagogy that respected this and 
    sought to build on the child\'s own motivation and initiatives," and "A concern about the child\'s capacity to 
    resist an ideology transmitted through the school." In his "Political Justice" he criticises state sponsored 
    schooling "on account of its obvious alliance with national government". Early American anarchist Josiah Warren 
    advanced alternative education experiences in the libertarian communities he established. Max Stirner wrote in 
    1842 a long essay on education called "The False Principle of our Education". In it Stirner names his educational 
    principle "personalist," explaining that self-understanding consists in hourly self-creation. Education for him 
    is to create "free men, sovereign characters," by which he means "eternal characters\xa0... who are therefore 
    eternal because they form themselves each moment".\n\nIn the United States "freethought was a basically 
    anti-christian, anti-clerical movement, whose purpose was to make the individual politically and spiritually free 
    to decide for himself on religious matters. A number of contributors to "Liberty" (anarchist publication) were 
    prominent figures in both freethought and anarchism. The individualist anarchist George MacDonald was a co-editor 
    of "Freethought" and, for a time, "The Truth Seeker". E.C. Walker was co-editor of the excellent free-thought / 
    free love journal "Lucifer, the Light-Bearer"". "Many of the anarchists were ardent freethinkers; reprints from 
    freethought papers such as "Lucifer, the Light-Bearer", "Freethought" and "The Truth Seeker" appeared in 
    "Liberty"...\xa0The church was viewed as a common ally of the state and as a repressive force in and of 
    itself".\n\nIn 1901, Catalan anarchist and free-thinker Francesc Ferrer i Guàrdia established "modern" or 
    progressive schools in Barcelona in defiance of an educational system controlled by the Catholic Church. The 
    schools\' stated goal was to "educate the working class in a rational, secular and non-coercive setting". 
    Fiercely anti-clerical, Ferrer believed in "freedom in education", education free from the authority of church 
    and state. Murray Bookchin wrote: "This period [1890s] was the heyday of libertarian schools and pedagogical 
    projects in all areas of the country where Anarchists exercised some degree of influence. Perhaps the best-known 
    effort in this field was Francisco Ferrer\'s Modern School (Escuela Moderna), a project which exercised a 
    considerable influence on Catalan education and on experimental techniques of teaching generally." La Escuela 
    Moderna, and Ferrer\'s ideas generally, formed the inspiration for a series of "Modern Schools" in the United 
    States, Cuba, South America and London. The first of these was started in New York City in 1911. It also inspired 
    the Italian newspaper "Università popolare", founded in 1901. Russian christian anarchist Leo Tolstoy established 
    a school for peasant children on his estate. Tolstoy\'s educational experiments were short-lived due to 
    harassment by the Tsarist secret police. Tolstoy established a conceptual difference between education and 
    culture. He thought that "Education is the tendency of one man to make another just like himself\xa0... Education 
    is culture under restraint, culture is free. [Education is] when the teaching is forced upon the pupil, 
    and when then instruction is exclusive, that is when only those subjects are taught which the educator regards as 
    necessary". For him "without compulsion, education was transformed into culture".\n\nA more recent libertarian 
    tradition on education is that of unschooling and the free school in which child-led activity replaces pedagogic 
    approaches. Experiments in Germany led to A. S. Neill founding what became Summerhill School in 1921. Summerhill 
    is often cited as an example of anarchism in practice. However, although Summerhill and other free schools are 
    radically libertarian, they differ in principle from those of Ferrer by not advocating an overtly political class 
    struggle-approach.\nIn addition to organising schools according to libertarian principles, anarchists have also 
    questioned the concept of schooling per se. The term deschooling was popularised by Ivan Illich, who argued that 
    the school as an institution is dysfunctional for self-determined learning and serves the creation of a consumer 
    society instead.\n\nCriticisms of anarchism include moral criticisms and pragmatic criticisms. Anarchism is often 
    evaluated as unfeasible or utopian by its critics.\n\n\n' """


def test_data_source():
    from nhmr.mapreduce import DataSource

    stage_1 = _test_data.split("\n")
    stage_2 = [m.strip() for m in stage_1]
    stage_3 = [m for m in stage_2 if len(m) > 0]

    test_sentences = DataSource(stage_3)

    for entry, truth in zip(test_sentences, stage_3):
        assert entry == truth, f"{entry} != {truth}"


def test_map_job():
    from nhmr.mapreduce import DataSource

    stage_1 = _test_data.split("\n")
    stage_2 = [m.strip() for m in stage_1]
    stage_3 = [m for m in stage_2 if len(m) > 0]

    test_sentences = DataSource([_test_data])\
        .flat_map(lambda x: x.split("\n"))\
        .map(lambda x: x.strip())\
        .filter(lambda x: len(x) > 0)

    for entry, truth in zip(test_sentences, stage_3):
        assert entry == truth, f"{entry} != {truth}"


def test_cache_job():
    from nhmr.mapreduce import DataSource

    stage_1 = _test_data.split("\n")
    stage_2 = [m.strip() for m in stage_1]
    stage_3 = [m for m in stage_2 if len(m) > 0]

    DataSource([_test_data])\
        .flat_map(lambda x: x.split("\n"))\
        .cache("stage_1")\
        .map(lambda x: x.strip()) \
        .cache("stage_2") \
        .filter(lambda x: len(x) > 0)\
        .persist("stage_3")

    for entry, truth in zip(open("stage_3/part_00000"), stage_3):
        entry = entry.rstrip("\n")
        assert entry == truth, f"{repr(entry)} != {repr(truth)}"

    from_cache = DataSource([]) \
        .cache("stage_2") \
        .filter(lambda x: len(x) > 0)

    for entry, truth in zip(from_cache, stage_3):
        entry = entry.rstrip("\n")
        assert entry == truth, f"{repr(entry)} != {repr(truth)}"

    shutil.rmtree("stage_1")
    shutil.rmtree("stage_2")
    shutil.rmtree("stage_3")


# def test_shuffler():
#     from nhmr.mapreduce import DataSource
#     from nhmr.mapreduce import Shuffler
#     from pathlib import Path
#     from collections import Counter
#
#     words = DataSource([_test_data]) \
#         .flat_map(lambda x: x.split("\n")) \
#         .map(lambda x: x.strip()) \
#         .filter(lambda x: len(x) > 0) \
#         .flat_map(lambda x: x.split())
#
#     shuffler = Shuffler(Path("shuffler"), pre_shuffle_shard_size=100)
#     shuffler.write_sorted(Counter(words))
#     for key, val in shuffler.get_sorted():
#         pass
#         # print(key, val)
#
#     shutil.rmtree("shuffler")


def test_reduce():
    from nhmr.mapreduce import DataSource
    from collections import Counter

    words = DataSource([_test_data]) \
        .flat_map(lambda x: x.split("\n")) \
        .map(lambda x: x.strip()) \
        .filter(lambda x: len(x) > 0) \
        .flat_map(lambda x: x.split())

    words = list(words)
    word_count_ = Counter(words)

    word_count = DataSource(words) \
        .map(lambda x: (x, 1))\
        .reduce(lambda x, y: x + y)

    observed = 0
    for key, value in word_count:
        assert value == word_count_[key]
        observed += 1
    assert observed == len(word_count_)


def test_sort():
    from nhmr import DataSource

    sorted_items = DataSource(reversed(range(1000))).sort(chunk_size_lines=10)
    for i, s in zip(range(1000), sorted_items):
        assert i == s

    sorted_items = DataSource(range(1000)).sort(chunk_size_lines=10, ascending=False)
    for i, s in zip(reversed(range(1000)), sorted_items):
        assert i == s


# def split_lines(x):
#     return x.split("\n")
#
#
# def strip_lines(x):
#     return x.strip()
#
#
# def split_words(x):
#     return x.split()
#
#
# def assign_count(x):
#     return (x, 1)
#
#
# tokenizer = RegexpTokenizer("[\w][\w-]+[\w]|[\w]\.|[\w]+|[^\w\s]|[0-9]+")
# def tokenize(x):
#     return tokenizer.tokenize(x)
#
#
# def process(x_):
#     counter = Counter()
#     for x in x_.split("\n"):
#         x = x.strip()
#         if len(x) == 0:
#             continue
#         for tok in tokenize(x):
#             counter[tok] += 1
#     return list(counter.items())


def test_wordcount():
    from nhmr.mapreduce import DataSource

    DataSource([_test_data]) \
        .flat_map(lambda x: x.split("\n")) \
        .map(lambda x: x.strip()) \
        .filter(lambda x: len(x) > 0) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduce(lambda x, y: x + y) \
        .persist("wordcount", serialize_fn=lambda x: f"{x[0]}\t{x[1]}")

    DataSource([_test_data]) \
        .flat_map(lambda x: x.split("\n")) \
        .map(lambda x: x.strip()) \
        .filter(lambda x: len(x) > 0) \
        .flat_map(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduce(lambda x, y: x + y, memory_check_frequency=1, taken_memory_thresh_mb=0.0001) \
        .persist("wordcount1", serialize_fn=lambda x: f"{x[0]}\t{x[1]}")

    for entry, truth in zip(open("wordcount/part_00000"), open("wordcount1/part_00000")):
        assert entry == truth, f"{repr(entry)} != {repr(truth)}"

    shutil.rmtree("wordcount")
    shutil.rmtree("wordcount1")


if __name__ == "__main__":
    # test_data_source()
    # test_map_job()
    # test_cache_job()
    # test_sort()
    # test_shuffler()
    test_reduce()
    test_wordcount()

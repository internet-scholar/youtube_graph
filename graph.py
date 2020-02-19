import networkx as nx
import csv
import community
from internet_scholar import AthenaDatabase, compress, AthenaLogger, decompress
from datetime import date, timedelta, datetime
import boto3
import numpy
from xml.etree.ElementTree import Element, SubElement
from xml.etree import ElementTree
from xml.dom import minidom


SELECT_EDGES = """
with auxiliary_twitter_youtube_view as
(
  SELECT DISTINCT
    tweet_user_url.user_id user_id,
    youtube_video_snippet.snippet.channelid channel_id,
    tweet_user_url.creation_date creation_date
  FROM
    tweet_user_url,
    validated_url,
    youtube_video_snippet
  WHERE
    validated_url.url = tweet_user_url.url AND
    url_extract_host(validated_url.validated_url) = 'www.youtube.com' AND
    youtube_video_snippet.snippet.channelid IS NOT NULL AND
    url_extract_parameter(validated_url.validated_url, 'v') = youtube_video_snippet.id
)
SELECT
  a.channel_id source_id,
  b.channel_id target_id,
  count(distinct a.user_id) as Weight
FROM
  auxiliary_twitter_youtube_view a,
  auxiliary_twitter_youtube_view b
WHERE
  a.user_id = b.user_id AND
  a.channel_id < b.channel_id AND
  a.creation_date between '{initial_date}' and '{final_date}' AND
  b.creation_date between '{initial_date}' and '{final_date}'
GROUP BY
  a.channel_id,
  b.channel_id
HAVING
  count(distinct a.user_id) >= {min_users}
ORDER BY
  source_id,
  target_id
"""

CREATE_YOUTUBE_GRAPH_EDGE = """
CREATE EXTERNAL TABLE if not exists youtube_graph_edge (
   source_id string,
   target_id string,
   weight int
)
partitioned by (min_users int, timespan int, final_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'skip.header.line.count' = '1'
   )
STORED AS TEXTFILE
LOCATION 's3://{s3_data}/youtube_graph_edge/';
"""

MIN_DATE = """
SELECT 
  min(creation_date) min_date
FROM
  tweet_user_url
"""


def create_edges(min_users, timespan, final_date, end):
    s3 = boto3.resource('s3')
    athena_db = AthenaDatabase(database='internet_scholar', s3_output='internet-scholar-admin')
    min_date = athena_db.query_athena_and_get_result(query_string=MIN_DATE)['min_date']
    min_date = datetime.strptime(min_date, '%Y-%m-%d').date()
    initial_date = final_date - timedelta(days=timespan-1)
    while final_date <= end:
        print('Edges - {}'.format(str(final_date)))
        if initial_date >= min_date:
            edges = athena_db.query_athena_and_download(query_string=SELECT_EDGES.format(initial_date=str(initial_date),
                                                                                         final_date=str(final_date),
                                                                                         min_users=min_users),
                                                        filename='edges.csv')
            compressed_file = compress(filename=edges, delete_original=True)
            s3_filename = "youtube_graph_edge/min_users={min_users}/" \
                          "timespan={timespan}/final_date={final_date}/edges.csv.bz2".format(
                min_users=min_users,
                timespan=timespan,
                final_date=str(final_date))
            s3.Bucket('internet-scholar').upload_file(str(compressed_file), s3_filename)
        final_date = final_date + timedelta(days=1)
        initial_date = initial_date + timedelta(days=1)
    athena_db.query_athena_and_wait(query_string='drop table if exists youtube_graph_edge')
    athena_db.query_athena_and_wait(query_string=CREATE_YOUTUBE_GRAPH_EDGE.format(s3_data='internet-scholar'))
    athena_db.query_athena_and_wait(query_string='MSCK REPAIR TABLE youtube_graph_edge')


SELECT_NODES = """
with all_nodes as
(
  select source_id as channel_id
  from
    youtube_graph_edge
  where
    min_users = {min_users} and
    timespan = {timespan} and
    final_date = '{final_date}'
  UNION DISTINCT
  select target_id as channel_id
  from
    youtube_graph_edge
  where
    min_users = {min_users} and
    timespan = {timespan} and
    final_date = '{final_date}'
)
SELECT
  all_nodes.channel_id channel_id,
  max_by(replace(replace(replace(replace(replace(youtube_video_snippet.snippet.channelTitle, chr(10), ' '), '"', ' '), '\\', '-'), chr(13), ' '), chr(9), ' '),
         youtube_video_snippet.snippet.publishedAt) channel_title,
  coalesce(max(youtube_channel_stats.statistics.viewcount), 0) as cumulative_view_count,
  coalesce(max(youtube_channel_stats.statistics.viewcount), 0) -
  coalesce(min(youtube_channel_stats.statistics.viewcount), 0) as view_count,
  coalesce(max(youtube_channel_stats.statistics.videocount), 0) as cumulative_video_count,
  coalesce(max(youtube_channel_stats.statistics.videocount), 0) -
  coalesce(min(youtube_channel_stats.statistics.videocount), 0) as video_count,
  coalesce(max(youtube_channel_stats.statistics.commentcount), 0) as cumulative_comment_count,
  coalesce(max(youtube_channel_stats.statistics.commentcount), 0) -
  coalesce(min(youtube_channel_stats.statistics.commentcount), 0) as comment_count,
  coalesce(max(youtube_channel_stats.statistics.subscribercount), 0) as cumulative_subscriber_count,
  coalesce(max(youtube_channel_stats.statistics.subscribercount), 0) -
  coalesce(min(youtube_channel_stats.statistics.subscribercount), 0) as subscriber_count
FROM
  all_nodes inner join youtube_video_snippet on all_nodes.channel_id = youtube_video_snippet.snippet.channelid
  left join youtube_channel_stats on youtube_video_snippet.snippet.channelid = youtube_channel_stats.id and
       youtube_channel_stats.creation_date between '{initial_date}' and '{final_date}'
group by
  all_nodes.channel_id
"""

CREATE_YOUTUBE_GRAPH_NODE = """
CREATE EXTERNAL TABLE if not exists youtube_graph_node (
   channel_id string,
   channel_title string,
   cumulative_view_count bigint,
   view_count bigint,
   cumulative_video_count bigint,
   video_count bigint,
   cumulative_comment_count bigint,
   comment_count bigint,
   cumulative_subscriber_count bigint,
   subscriber_count bigint
)
partitioned by (min_users int, timespan int, final_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'skip.header.line.count' = '1'
   )
STORED AS TEXTFILE
LOCATION 's3://{s3_data}/youtube_graph_node/';
"""


def create_nodes(min_users, timespan, final_date, end):
    s3 = boto3.resource('s3')
    athena_db = AthenaDatabase(database='internet_scholar', s3_output='internet-scholar-admin')
    min_date = athena_db.query_athena_and_get_result(query_string=MIN_DATE)['min_date']
    min_date = datetime.strptime(min_date, '%Y-%m-%d').date()
    initial_date = final_date - timedelta(days=timespan-1)
    while final_date <= end:
        print('Nodes - {}'.format(str(final_date)))
        if initial_date >= min_date:
            edges = athena_db.query_athena_and_download(query_string=SELECT_NODES.format(initial_date=str(initial_date),
                                                                                         final_date=str(final_date),
                                                                                         min_users=min_users,
                                                                                         timespan=timespan),
                                                        filename='nodes.csv')
            compressed_file = compress(filename=edges, delete_original=True)
            s3_filename = "youtube_graph_node/min_users={min_users}/" \
                          "timespan={timespan}/final_date={final_date}/nodes.csv.bz2".format(
                min_users=min_users,
                timespan=timespan,
                final_date=str(final_date))
            s3.Bucket('internet-scholar').upload_file(str(compressed_file), s3_filename)
        final_date = final_date + timedelta(days=1)
        initial_date = initial_date + timedelta(days=1)
    athena_db.query_athena_and_wait(query_string='drop table if exists youtube_graph_node')
    athena_db.query_athena_and_wait(query_string=CREATE_YOUTUBE_GRAPH_NODE.format(s3_data='internet-scholar'))
    athena_db.query_athena_and_wait(query_string='MSCK REPAIR TABLE youtube_graph_node')


EDGES_LOUVAIN = """
SELECT
  source_id,
  target_id,
  weight
FROM
  youtube_graph_edge
where
  timespan = {timespan} and
  min_users = {min_users} and
  final_date = '{final_date}'
"""

CREATE_YOUTUBE_GRAPH_LOUVAIN = """
CREATE EXTERNAL TABLE if not exists youtube_graph_louvain (
   resolution float,
   channel_id string,
   cluster int,
   graph_size int,
   cluster_size int,
   cluster_count int
)
partitioned by (min_users int, timespan int, final_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"',
   'skip.header.line.count' = '1'
   )
STORED AS TEXTFILE
LOCATION 's3://{s3_data}/youtube_graph_louvain/';
"""


def create_louvain(min_users, timespan, final_date, end):
    s3 = boto3.resource('s3')
    athena_db = AthenaDatabase(database='internet_scholar', s3_output='internet-scholar-admin')
    while final_date <= end:
        print('Louvain - {}'.format(str(final_date)))
        edges = athena_db.query_athena_and_download(query_string=EDGES_LOUVAIN.format(final_date=str(final_date),
                                                                                      min_users=min_users,
                                                                                      timespan=timespan),
                                                    filename='edges_louvain.csv')
        g = nx.Graph()
        with open(edges, newline='', encoding="utf8") as csv_reader:
            reader = csv.DictReader(csv_reader)
            for edge in reader:
                g.add_edge(edge['source_id'],
                           edge['target_id'],
                           weight=int(edge['weight']))

        with open('./louvain.csv', 'w', encoding="utf8") as csv_writer:
            writer = csv.DictWriter(csv_writer,
                                    fieldnames=['resolution', 'channel_id', 'cluster',
                                                'graph_size', 'cluster_size', 'cluster_count'],
                                    dialect='unix')
            writer.writeheader()
            nodes = list(g)
            graph_size = len(nodes)
            for resolution in numpy.arange(10, 0, -0.1):
                partition = community.best_partition(g, resolution=resolution, randomize=False)
                cluster_count = len(set(partition.values()))
                for partition_number in set(partition.values()):
                    new_partition = list()
                    for channel_id in partition.keys():
                        if partition[channel_id] == partition_number:
                            new_partition.append(channel_id)
                    cluster_size = len(new_partition)
                    new_partition_number = nodes.index(min(new_partition))
                    for item in new_partition:
                        new_record = dict()
                        new_record['resolution'] = "{:.1f}".format(resolution)
                        new_record['channel_id'] = item
                        new_record['cluster'] = new_partition_number
                        new_record['graph_size'] = graph_size
                        new_record['cluster_size'] = cluster_size
                        new_record['cluster_count'] = cluster_count
                        writer.writerow(new_record)

        compressed_file = compress(filename='./louvain.csv', delete_original=True)
        s3_filename = "youtube_graph_louvain/min_users={min_users}/" \
                      "timespan={timespan}/final_date={final_date}/louvain.csv.bz2".format(
            min_users=min_users,
            timespan=timespan,
            final_date=str(final_date))
        s3.Bucket('internet-scholar').upload_file(str(compressed_file), s3_filename)
        final_date = final_date + timedelta(days=1)
    athena_db.query_athena_and_wait(query_string='drop table if exists youtube_graph_louvain')
    athena_db.query_athena_and_wait(query_string=CREATE_YOUTUBE_GRAPH_LOUVAIN.format(s3_data='internet-scholar'))
    athena_db.query_athena_and_wait(query_string='MSCK REPAIR TABLE youtube_graph_louvain')


def prettify_xml(elem):
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ", encoding="utf-8")


def create_gexf(min_users, timespan, final_date, end):
    s3 = boto3.resource('s3')
    while final_date <= end:
        print('GEXF - {}'.format(str(final_date)))
        gexf = Element('gexf', {'xmlns': "http://www.gexf.net/1.3",
                                'version': "1.3",
                                'xmlns:viz': "http://www.gexf.net/1.3/viz",
                                'xmlns:xsi': "http://www.w3.org/2001/XMLSchema-instance",
                                'xsi:schemaLocation': "http://www.gexf.net/1.3 http://www.gexf.net/1.3/gexf.xsd"})
        graph = SubElement(gexf, 'graph', {'mode': "dynamic",
                                           'defaultedgetype': "undirected",
                                           'timeformat': "double",
                                           'timerepresentation': "timestamp"})

        attributes = SubElement(graph, 'attributes', {'class': "node", 'mode': "static"})
        SubElement(attributes, 'attribute', {'id': '1', 'title': 'view_count', 'type': 'long'})
        SubElement(attributes, 'attribute', {'id': '2', 'title': 'cumulative_view_count', 'type': 'long'})
        SubElement(attributes, 'attribute', {'id': '3', 'title': 'subscriber_count', 'type': 'long'})
        SubElement(attributes, 'attribute', {'id': '4', 'title': 'cumulative_subscriber_count', 'type': 'long'})
        SubElement(attributes, 'attribute', {'id': '5', 'title': 'video_count', 'type': 'long'})
        SubElement(attributes, 'attribute', {'id': '6', 'title': 'cumulative_video_count', 'type': 'long'})

        attributes = SubElement(graph, 'attributes', {'class': "node", 'mode': "dynamic"})
        SubElement(attributes, 'attribute', {'id': '7', 'title': 'cluster', 'type': 'long'})
        nodes = SubElement(graph, 'nodes')
        edges = SubElement(graph, 'edges')

        s3_filename = "youtube_graph_node/min_users={min_users}/" \
                      "timespan={timespan}/final_date={final_date}/nodes.csv.bz2".format(
            min_users=min_users,
            timespan=timespan,
            final_date=str(final_date))
        s3.Bucket('internet-scholar').download_file(s3_filename, './nodes.csv.bz2')
        nodes_file = decompress(filename='./nodes.csv.bz2')
        with open(nodes_file, newline='', encoding="utf8") as csv_reader:
            reader = csv.DictReader(csv_reader)
            dict_attvalues = dict()
            for node_record in reader:
                node = SubElement(nodes, 'node', {'id': node_record['channel_id'],
                                                  'label': node_record['channel_title']})
                dict_attvalues[node_record['channel_id']] = SubElement(node, 'attvalues')
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '1',
                                                                                   'value': node_record['view_count']})
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '2',
                                                                                   'value': node_record['cumulative_view_count']})
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '3',
                                                                                   'value': node_record['subscriber_count']})
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '4',
                                                                                   'value': node_record['cumulative_view_count']})
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '5',
                                                                                   'value': node_record['video_count']})
                SubElement(dict_attvalues[node_record['channel_id']], 'attvalue', {'for': '6',
                                                                                   'value': node_record['cumulative_video_count']})

        s3_filename = "youtube_graph_louvain/min_users={min_users}/" \
                      "timespan={timespan}/final_date={final_date}/louvain.csv.bz2".format(
            min_users=min_users,
            timespan=timespan,
            final_date=str(final_date))
        s3.Bucket('internet-scholar').download_file(s3_filename, './louvain.csv.bz2')
        louvain_file = decompress(filename='./louvain.csv.bz2')
        with open(louvain_file, newline='', encoding="utf8") as csv_reader:
            reader = csv.DictReader(csv_reader)
            for louvain_record in reader:
                SubElement(dict_attvalues[louvain_record['channel_id']], 'attvalue', {'for': '7',
                                                                                      'value': louvain_record['cluster'],
                                                                                      'timestamp': louvain_record['resolution']})

        s3_filename = "youtube_graph_edge/min_users={min_users}/" \
                      "timespan={timespan}/final_date={final_date}/edges.csv.bz2".format(
            min_users=min_users,
            timespan=timespan,
            final_date=str(final_date))
        s3.Bucket('internet-scholar').download_file(s3_filename, './edges.csv.bz2')
        edges_file = decompress(filename='./edges.csv.bz2')
        with open(edges_file, newline='', encoding="utf8") as csv_reader:
            reader = csv.DictReader(csv_reader)
            for edge_record in reader:
                SubElement(edges, 'edge', {'source': edge_record['source_id'],
                                           'target': edge_record['target_id'],
                                           'Weight': edge_record['Weight']})

        f = open('./network.gexf', 'wb')
        f.write(prettify_xml(gexf))
        f.close()
        compressed_gexf = compress(filename='./network.gexf')
        s3_filename = "youtube_graph_gexf/min_users={min_users}/" \
                      "timespan={timespan}/final_date={final_date}/network.gexf.bz2".format(min_users=min_users,
                                                                                            timespan=timespan,
                                                                                            final_date=str(final_date))
        s3.Bucket('internet-scholar').upload_file(str(compressed_gexf), s3_filename)

        final_date = final_date + timedelta(days=1)


def main():
    logger = AthenaLogger(app_name="youtube_analysis",
                          s3_bucket='internet-scholar-admin',
                          athena_db='internet_scholar_admin')
    try:
        min_users = 3
        timespan = 60

        # final_date = date(2019, 10, 13)
        # end = date(2019, 10, 14)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)

        final_date = date(2019, 10, 15)
        end = date(2019, 10, 31)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)

        final_date = date(2019, 11, 1)
        end = date(2019, 11, 30)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)

        final_date = date(2019, 12, 1)
        end = date(2019, 12, 31)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)

        final_date = date(2020, 1, 1)
        end = date(2020, 1, 31)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)

        final_date = date(2020, 2, 1)
        end = date(2020, 2, 17)
        # create_edges(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        # create_nodes(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_louvain(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
        create_gexf(min_users=min_users, timespan=timespan, final_date=final_date, end=end)
    finally:
        logger.save_to_s3()
        logger.recreate_athena_table()


if __name__ == '__main__':
    main()

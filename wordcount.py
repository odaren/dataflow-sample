import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

class WordExtractingDoFn(beam.DoFn):
    """Parse each line of input text into words."""
    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)

def run(argv=None, save_main_session=True):
    # コマンドライン引数から、出力先を受け取る
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',default='gs://dataflow-samples/shakespeare/kinglear.txt')
    parser.add_argument('--output',required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

     # GCP オプション
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = ''  # プロジェクトID
    google_cloud_options.region = 'asia-northeast1'  # プロジェクトID
    google_cloud_options.job_name = 'wordcount-test'  # 任意のジョブ名
    google_cloud_options.staging_location = ''  # ファイルをステージングするための GCS パス
    google_cloud_options.temp_location = ''  # 一時ファイルの GCS パス

    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner' 

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as p:

        # テキストファイルを読み込んでPCollectionとしてlinesを生成
        lines = p | 'Read' >> ReadFromText(known_args.input)

        # 文字を単語ごとにカウントするTransform処理
        counts = (
            lines
            # Split : 分を単語ごとに分割
            | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
            # PairWithOne : (単語, 1)というマップを生成
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            # GroupAndSum : 単語をキーにして、件数を集計
            | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # PCollectionであるcountsをオブジェクトストレージに出力
        counts | WriteToText(known_args.output)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

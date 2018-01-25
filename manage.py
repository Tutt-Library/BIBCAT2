__author__ = "Jeremy Nelson"
import datetime
import click
import pymarc

@click.command()
@click.option("--path", help="Full path including file name to MARC21")
@click.option("--shard_size", default=10000, 
              help="Shard size, default is 100,000")
def shard(path, shard_size):
    start, counter, errors, shards = datetime.datetime.utcnow(), 0, [], 0
    click.echo("Starting sharding of {} at {}".format(path, start))
    marc_reader = pymarc.MARCReader(open(path, 'rb'),
                    to_unicode=True,
                    utf8_handling='ignore')
    marcshard_path = "E:/2018/tiger-catalog/tmp/marc-{}-{}k.mrc".format(
        counter,
        shard_size)
    marc_writer = pymarc.MARCWriter(open(marcshard_path, 'wb+'))
    while 1:
        
        try:
            
            rec = next(marc_reader)
            rec.force_utf8 = True
            marc_writer.write(rec)
            if not counter%shard_size and counter > 0:
                shards += 1
                marc_writer.close()
                marc_writer = pymarc.MARCWriter(
                    open("E:/2018/tiger-catalog/tmp/marc-{}-{}k.mrc".format(
        counter,
        counter + shard_size),
                         "wb+"))
            
        except:
            errors.append(counter)
            click.echo("e{:,}".format(counter), nl=False)
        if not counter%1000 and counter > 0:
            click.echo(".", nl=False)
        if not counter%25000 and counter > 0:
            click.echo("{:,}".format(counter), nl=False)
        counter += 1
    marc_writer.close()
    end = datetime.datetime.utcnow()
    click.echo("""Finished at {}, sharded {:,} MARC records in {:,} minutes
Total Errors: {:,}""".format(
        end,
        counter,
        (end-start).seconds / 60.0),
          len(errors))
    return errors

if __name__ == "__main__":
    shard()

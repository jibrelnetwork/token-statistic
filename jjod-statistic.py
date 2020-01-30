from datetime import datetime
from typing import Tuple, Dict, List, Optional
import traceback
import time
import sys

import requests


TOKEN_ADDRESS = '0x94157579d1853d85d90447c4c3217dc63799ab08'
ETHBE_URL = 'https://ethbe.api.jsearch.io'
BLOCK_INFO = '{ethbe_url}/v1/blocks/{tag}'
TOKEN_TRANSFERS = '{ethbe_url}/v1/tokens/{token_address}/transfers?order=desc&block_number={block_number}'
TOKEN_HOLDERS = '{ethbe_url}/v1/tokens/{token_address}/holders?blockchain_tip={block_hash}&order=desc'


def get_data(url: str) -> Optional[Dict]:
    max_attempts = 10

    for attempt in range(max_attempts):
        # noinspection PyBroadException
        try:
            response = requests.get(url)
            response.raise_for_status()
            break
        except Exception:
            exception_str = ''.join(traceback.format_exception(*sys.exc_info()))
            if attempt < max_attempts - 1:
                print(
                    "Failed to get content from url '{}' due to error. Sleep and try again.\n{}"
                ).format(url, exception_str)
                time.sleep(3)
            else:
                print(
                    "Failed to get content from url '{}' due to error. Sleep and try again.\n{}"
                ).format(url, exception_str)
                return None

    return response.json()


def get_block(tag: str) -> Optional[Dict]:
    """
    :param: tag - block number, block hash or keyword latest
    :return: block data
    """
    data = get_data(BLOCK_INFO.format(
        ethbe_url=ETHBE_URL,
        tag=tag,
    ))

    return data['data'] if 'data' in data else None


def get_token_holders(token_address: str, block_hash: str, data_cur: str = None) -> Tuple[bool, Optional[List[Dict]]]:
    """
    :param token_address: token address
    :param block_hash: block hash
    :return: True if next link is available and List of holders
    """
    data = get_data(
        TOKEN_HOLDERS.format(
            ethbe_url=ETHBE_URL,
            token_address=token_address,
            block_hash=block_hash,
        ) if data_cur is None else data_cur
    )

    is_next_available = True \
        if 'paging' in data and 'next' in data['paging'] and data['paging']['next'] is not None else False
    next_url = ETHBE_URL + data['paging']['next'] if is_next_available else None
    holders_list = data['data'] if 'data' in data else None

    return is_next_available, next_url, holders_list


def get_token_transfers(
        token_address: str,
        block_number: str,
        data_cur: str = None
) -> Tuple[bool, Optional[List[Dict]]]:
    """
    :param token_address: token address
    :param block_number: block number
    :return: True if next link is available and List of transfers
    """
    data = get_data(
        TOKEN_TRANSFERS.format(
            ethbe_url=ETHBE_URL,
            token_address=token_address,
            block_number=block_number,
        ) if data_cur is None else data_cur
    )

    is_next_available = True \
        if 'paging' in data and 'next' in data['paging'] and data['paging']['next'] is not None else False
    next_url = ETHBE_URL + data['paging']['next'] if is_next_available else None
    transfers_list = data['data'] if 'data' in data else None

    return is_next_available, next_url, transfers_list


def write_table_1(block_number: int, block_timestamp: int):
    """
    jjood-transfers-YYYY-mm-dd-HH-mm.csv
    From, To, Amount, Date, TX hash
    """
    block_time = datetime.fromtimestamp(block_timestamp)
    output_file_name = f'jjood-transfers-{block_time.strftime("%Y-%m-%d-%H-%M")}.csv'
    with open(output_file_name, "w") as f:
        f.write('From,To,Amount,Date,TX hash,\n')
        is_next = True
        data_cur = None
        while is_next:
            is_next, data_cur, transfers = get_token_transfers(TOKEN_ADDRESS, block_number, data_cur)
            for transfer in transfers:
                print(transfer)
                transfer_time = datetime.fromtimestamp(transfer["timestamp"])
                f.write(
                    f'{transfer["from"]},'
                    f'{transfer["to"]},'
                    f'{float(transfer["amount"])/10**18},'
                    f'{transfer_time.strftime("%Y-%m-%d %H:%M:%S")},'
                    f'{transfer["transactionHash"]},\n'
                )


def write_table_2(block_hash: str, block_timestamp: int):
    """
    jjod-holders-YYYY-mm-dd-HH-mm.csv"
    address, balance
    """
    block_time = datetime.fromtimestamp(block_timestamp)
    output_file_name = f'jjod-holders-{block_time.strftime("%Y-%m-%d-%H-%M")}.csv'
    with open(output_file_name, "w") as f:
        f.write('address,balance,\n')
        is_next = True
        data_cur = None
        while is_next:
            is_next, data_cur, holders = get_token_holders(TOKEN_ADDRESS, block_hash, data_cur)
            for holder in holders:
                print(holder)
                f.write(
                    f'{holder["accountAddress"]},'
                    f'{float(holder["balance"]) / 10 ** 18},\n'
                )


if __name__ == "__main__":
    print('Start...')

    latest_block_number = get_block('latest')['number']
    block = get_block(latest_block_number - 6)

    print('write_table_1')
    write_table_1(block['number'], block['timestamp'])

    print('write_table_2')
    write_table_2(block['hash'], block['timestamp'])

    print('Finish')

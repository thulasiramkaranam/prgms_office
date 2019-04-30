import logging
from app.function2 import Function
from app.variables import FUNCTION_NAME


# Setup logging
log_format = '%(asctime)s [%(filename)s:%(lineno)s - %(funcName)s()] [%(levelname)s] %(message)s'
logging.basicConfig(format=log_format)
logger = logging.getLogger(FUNCTION_NAME)
logger.setLevel(logging.INFO)


def return_error(msg, req_id):
    return {
        'statusCode': '500',
        'body': {
            'error': msg
        },
        'context': {
            'req_id': req_id
        },
        'headers': {
            'Content-Type': 'application/json',
        }
    }


def lambda_handler(event, context):
    logger.info("In main.handler")
    try:
        response = Function.run(event, context)
        logger.info("Returning response")
        # import json
        logger.info(response)
        # logger.info(print(json.dumps(response, indent=4, sort_keys=True)))
        return response
    except Exception as e:
        import traceback
        info = traceback.format_exc()
        logging.error(info)
        return return_error("Something went wrong", context.aws_request_id)


if __name__ == '__main__':
    # event = {
    #     'events': [
    #         {
    #             'lat': 38.966675,
    #             'lng': -94.616898
    #         },
    #         {
    #             'lat': 38.966675,
    #             'lng': -94.616898
    #         }
    #     ]
    # }
    event = {
        'events': ["-1", "RC_EC_1000397", "EC_100127", "201903181496", "201903181746", "36", "49", "33", "22", "18", "50", "RC_EC_1000395", "201903182107", "201903181311", "16", "40", "13", "201903181320", "RC_EC_1000128", "54", "201903182110", "48", "9", "201903182068", "31", "201903182086", "EC_100134", "201903182103", "201903181858", "RC_EC_1000398", "38", "MR_EC_1000011", "28", "201903143701", "53", "EC_100122", "201903181944", "201903180358", "30", "35", "RC_EC_1000125", "52", "24", "RC_EC_1000143", "201903180438", "27", "44", "32", "41", "51", "201903181160", "201903180945", "201903181007", "34", "201903150545", "23", "MR_EC_1000009", "58", "46", "EC_100131", "19", "EC_100129", "201903181388", "201903180867", "26", "201903182128", "201903182056", "EC_100125", "55", "56", "11", "RC_EC_1000148", "201903182051", "20", "39", "RC_EC_1000162", "45", "201903180405", "37", "29", "21", "25", "12", "17", "10", "57", "47", "15", "42", "43", "14", "MR_EC_1000010", "201903181486"]
    }
    context = {}
    lambda_handler(event, context)

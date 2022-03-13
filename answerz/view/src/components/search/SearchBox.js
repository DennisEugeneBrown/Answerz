import {Col, Container, Row} from "react-grid-system";
import ReactVoiceInput from "react-voice-input";
import Checkbox from "@mui/material/Checkbox";
import {CSVReader} from "react-papaparse";
import Button from "@mui/material/Button";
import React, {useState} from "react";
import './SearchBox.scss'

export function SearchBox({onInputChanged, input, onError, submit, submit2}) {

    const [getIsScript, setIsScript] = useState(false)
    const [getScriptQueries, setScriptQueries] = useState([]);
    const [getScriptIndex, setScriptIndex] = useState(0);

    const onResult = (result) => {
        onInputChanged(result.replace('  ', ' '));
    }

    const handleChange = event => {
        onInputChanged(event.target.value.replace('  ', ' '));
    }

    const handleScriptBoxChange = () => setIsScript(!getIsScript);

    const handleOnDrop = (data) => {
        let column_headers = [];
        let script_queries = [];
        data.forEach((value, i) => {
            if (i === 0) {
                column_headers = value.data;
                return;
            }
            console.log(value.data[column_headers.indexOf('query')]);
            const query = value.data[column_headers.indexOf('query')];
            if (query) {
                script_queries.push(query);
            }
        });
        setScriptQueries(script_queries);
    }

    const handleOnError = (err) => {
        onError(err);
    }

    const handleOnRemoveFile = () => {
        setScriptQueries([]);
    }

    const runScript = () => {
        if (getScriptIndex < getScriptQueries.length) {
            onInputChanged(getScriptQueries[getScriptIndex]);
            setScriptIndex(getScriptIndex + 1);
            submit(getScriptQueries[getScriptIndex]);
        }
    }

    const handleSubmit = event => {
        event.preventDefault();
        submit(input);
    }

    return (
        <div className="search-container">

            <div className="search-container__input">
                <div className='search-container__input__voice'>
                    <ReactVoiceInput
                        onResult={onResult}
                        onEnd={submit2}
                    >
                    </ReactVoiceInput>
                </div>
                <div className='search-container__input__text'>
                    <form onSubmit={handleSubmit}>
                        <input className='search-container__input__text__input-field' type='text'
                               placeholder='Speak or Start Typing..'
                               value={input}
                               onChange={handleChange}/>
                    </form>
                </div>

            </div>


            <Row>
                <Col>
                    <Container className='Script'><Checkbox label={'Use Script'}
                                                            defaultUnchecked
                                                            onChange={handleScriptBoxChange}/> Script
                        {getIsScript ?
                            <CSVReader
                                onDrop={handleOnDrop}
                                onError={handleOnError}
                                addRemoveButton
                                removeButtonColor='#659cef'
                                onRemoveFile={handleOnRemoveFile}
                            >
                                <span>Drop CSV file here or click to upload.</span>
                            </CSVReader> : ''}
                        {getIsScript ?
                            getScriptQueries.map((d) => <li>{d}</li>) : ''}
                        {getIsScript ? <Button variant="contained"
                                               onClick={runScript}>Next</Button> : ''}
                    </Container>
                </Col>
            </Row>
        </div>
    )
}
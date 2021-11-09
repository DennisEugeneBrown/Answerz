import React, {useEffect, useState, useReducer} from 'react';
import Popup from 'reactjs-popup';
import 'reactjs-popup/dist/index.css';
import spinner from './spinner.svg';
import './App.css';
import 'react-minimal-side-navigation/lib/ReactMinimalSideNavigation.css';
import axios from 'axios'
import {JsonToTable} from "react-json-to-table";
import {Container, Row, Col} from 'react-grid-system';
import Checkbox from '@mui/material/Checkbox';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Typography from '@mui/material/Typography';
import Modal from '@mui/material/Modal';
import ReactVoiceInput from 'react-voice-input'

const formReducer = (state, event) => {
    return {
        ...state,
        [event.name]: event.value
    }
}


// Make sure to bind modal to your appElement (https://reactcommunity.org/react-modal/accessibility/)
console.log(document.getElementById('main'))

function App() {

    const [getMessage, setGetMessage] = useState({})
    const [getTable, setGetTable] = useState({})
    const [formData, setFormData] = useReducer(formReducer, {});
    const [submitting, setSubmitting] = useState(false);
    const [getError, setError] = useState(false);
    const [getListening, setListening] = useState(false);
    const [getColumns, setColumns] = useState([]);
    const [getDataTables, setDataTables] = useState([]);
    const [getState, setState] = useState({
        leftOpen: true,
        rightOpen: true,
    });

    const [getInputState, setInputState] = useState('');

    useEffect(() => {
        axios.get('http://localhost:5000/columns').then(response => {
            setColumns(response)
        }).catch(error => {
            console.log(error)
        })
    }, [])


    useEffect(() => {
        axios.get('http://localhost:5000/tables').then(response => {
            setDataTables(response)
        }).catch(error => {
            console.log(error)
        })


    }, [])


    let rows = []
    let cols = []

    const handleSubmit = event => {
        event.preventDefault();
        setSubmitting(true);
        axios.post('http://localhost:1234/answerz', {'text': getInputState})
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const handleChange = event => {
        // setFormData({
        //     name: event.target.name,
        //     value: event.target.value,
        // });
        setInputState(event.target.value);
    }

    const style = {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        width: 400,
        bgcolor: 'background.paper',
        border: '2px solid #000',
        boxShadow: 24,
        p: 4,
    };

    function update_table() {
        let table = getTable;
        table.message.append("")
    }

    function onResult(result) {
        setInputState(result);
    }

    function startListening() {
        setListening(true);
    }

    const handleSubmit2 = () => {
        setListening(false);
        setSubmitting(true);
        axios.post('http://localhost:1234/answerz', {'text': getInputState})
            .then(response => {
                console.log("Responded", response)
                setGetTable(response)
                setSubmitting(false);
                setError(false);
            }).catch((error) => {
            setSubmitting(false);
            setError(true);
        });
    }

    const [open, setOpen] = React.useState(false);
    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);
    const handleBoxChange = () => setGetTable(getTable);

    const toggleSidebar = (event) => {
        let key = `${event.currentTarget.parentNode.id}Open`;
        let otherKey = (key === 'leftOpen') ? 'rightOpen' : 'leftOpen';
        setState({[key]: !getState[key], [otherKey]: getState[otherKey]});
        console.log(getState);
    }

    const onEnd = () => {
        console.log('on end')
    }

    let leftOpen = getState.leftOpen ? 'open' : 'closed';
    let rightOpen = getState.rightOpen ? 'open' : 'closed';
    let languages = ['test', 'test2']
    let label = 'test'

    return (
        <div className="App">
            <div id='layout'>
                <div id='left' className={leftOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${leftOpen}`}>
                            <div className='header'>
                                <h2 className='titleLeft'>
                                    Data Panel
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Source Table</h3>
                                    </div>
                                </div>
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Data Tables</h3>
                                    </div>
                                </div>
                                {
                                    getDataTables.status === 200 ?
                                        getDataTables.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                                <div className='header'>
                                    <div className='item'>
                                        <h3>Columns</h3>
                                    </div>
                                </div>
                                {
                                    getColumns.status === 200 ?
                                        getColumns.data.map((item, key) => {
                                            return <div className='header'>
                                                <p className='listItem'>{item}</p>
                                            </div>
                                        }) : ''
                                }
                            </div>
                        </div>
                    </div>
                </div>

                <div id='main'>
                    <div className='header'>
                        <div className={`
                      title
                      ${'left-' + leftOpen}
                      ${'right-' + rightOpen}
                  `}>
                            <Container>
                                <div className='searchBar'>
                                    <Row>
                                        {/*<Col sm={1}>*/}

                                        {/*    <div className='mic'>*/}
                                        {/*        <svg preserveAspectRatio="none" width="22" height="34"*/}
                                        {/*             viewBox="0 0 22 34"*/}
                                        {/*             fill="none"*/}
                                        {/*             xmlns="http://www.w3.org/2000/svg">*/}
                                        {/*            <path*/}
                                        {/*                d="M4.25 6.375C4.25 4.68425 4.92165 3.06274 6.11719 1.86719C7.31274 0.67165 8.93425 0 10.625 0C12.3158 0 13.9373 0.67165 15.1328 1.86719C16.3284 3.06274 17 4.68425 17 6.375V17C17 18.6908 16.3284 20.3123 15.1328 21.5078C13.9373 22.7034 12.3158 23.375 10.625 23.375C8.93425 23.375 7.31274 22.7034 6.11719 21.5078C4.92165 20.3123 4.25 18.6908 4.25 17V6.375Z"*/}
                                        {/*                fill="url(#paint0_linear_638:277)"></path>*/}
                                        {/*            <path*/}
                                        {/*                d="M1.0625 13.8125C1.34429 13.8125 1.61454 13.9244 1.8138 14.1237C2.01306 14.323 2.125 14.5932 2.125 14.875V17C2.125 19.2543 3.02053 21.4163 4.61459 23.0104C6.20865 24.6045 8.37066 25.5 10.625 25.5C12.8793 25.5 15.0413 24.6045 16.6354 23.0104C18.2295 21.4163 19.125 19.2543 19.125 17V14.875C19.125 14.5932 19.2369 14.323 19.4362 14.1237C19.6355 13.9244 19.9057 13.8125 20.1875 13.8125C20.4693 13.8125 20.7395 13.9244 20.9388 14.1237C21.1381 14.323 21.25 14.5932 21.25 14.875V17C21.25 19.634 20.2717 22.1741 18.5048 24.1276C16.7378 26.081 14.3083 27.3085 11.6875 27.5719V31.875H18.0625C18.3443 31.875 18.6145 31.9869 18.8138 32.1862C19.0131 32.3855 19.125 32.6557 19.125 32.9375C19.125 33.2193 19.0131 33.4895 18.8138 33.6888C18.6145 33.8881 18.3443 34 18.0625 34H3.1875C2.90571 34 2.63546 33.8881 2.4362 33.6888C2.23694 33.4895 2.125 33.2193 2.125 32.9375C2.125 32.6557 2.23694 32.3855 2.4362 32.1862C2.63546 31.9869 2.90571 31.875 3.1875 31.875H9.5625V27.5719C6.9417 27.3085 4.51217 26.081 2.74524 24.1276C0.978317 22.1741 -3.30898e-05 19.634 8.39388e-10 17V14.875C8.39388e-10 14.5932 0.111942 14.323 0.311199 14.1237C0.510457 13.9244 0.780707 13.8125 1.0625 13.8125Z"*/}
                                        {/*                fill="url(#paint1_linear_638:277)"></path>*/}
                                        {/*            <defs>*/}
                                        {/*                <linearGradient id="paint0_linear_638:277"*/}
                                        {/*                                x1="10.625"*/}
                                        {/*                                y1="0"*/}
                                        {/*                                x2="10.625"*/}
                                        {/*                                y2="23.375"*/}
                                        {/*                                gradientUnits="userSpaceOnUse">*/}
                                        {/*                    <stop stop-color="#013E7A"></stop>*/}
                                        {/*                    <stop offset="1"*/}
                                        {/*                          stop-color="#0E022A"></stop>*/}
                                        {/*                </linearGradient>*/}
                                        {/*                <linearGradient id="paint1_linear_638:277"*/}
                                        {/*                                x1="10.625"*/}
                                        {/*                                y1="13.8125"*/}
                                        {/*                                x2="10.625"*/}
                                        {/*                                y2="34"*/}
                                        {/*                                gradientUnits="userSpaceOnUse">*/}
                                        {/*                    <stop stop-color="#013E7A"></stop>*/}
                                        {/*                    <stop offset="1"*/}
                                        {/*                          stop-color="#0E022A"></stop>*/}
                                        {/*                </linearGradient>*/}
                                        {/*            </defs>*/}
                                        {/*        </svg>*/}
                                        {/*    </div>*/}
                                        {/*</Col>*/}
                                        <Col sm={1}>
                                            <div className='mic'>
                                                <ReactVoiceInput
                                                    onSpeechStart={startListening}
                                                    onResult={onResult}
                                                    onEnd={handleSubmit2}
                                                >
                                                </ReactVoiceInput>
                                            </div>
                                        </Col>
                                        <Col sm={1}>
                                            <form className='input' onSubmit={handleSubmit}>

                                                <div className='input'>
                                                    <input className='input' type='text' value={getInputState}
                                                           onChange={handleChange}/>
                                                </div>
                                            </form>
                                        </Col>
                                        {/*<form className='input' onSubmit={handleSubmit}>*/}
                                        {/*    <input autoComplete="off" name="name"*/}
                                        {/*           placeholder='Speak or Type Command Here'*/}
                                        {/*           onChange={handleChange}/>*/}
                                        {/*</form>*/}
                                    </Row>
                                </div>
                            </Container>
                        </div>
                    </div>
                    <header className="App-header">

                        <div className='loading'>{submitting ?
                            <div>
                                {/*<h3>LOADING</h3>*/}
                                <img src={spinner} alt="logo"/>
                            </div>
                            : getListening ?
                                'Listening..'
                                :
                                ''}</div>
                        {getListening ?
                            ''
                            :
                            <div className='table'>{!submitting ?
                                getTable.status === 200 && !getError ?
                                    <div>
                                        {getTable.data.message.map((value, index) =>
                                            <div className='table-class'> {getTable.data.queries[index]} <JsonToTable
                                                json={value}/><Button
                                                onClick={handleOpen}>Table Properties</Button></div>)}
                                        <Modal
                                            open={open}
                                            onClose={handleClose}
                                            aria-labelledby="modal-modal-title"
                                            aria-describedby="modal-modal-description"
                                        >
                                            <Box sx={style}>
                                                <Typography id="modal-modal-title" variant="h6" component="h2">
                                                    Table Properties
                                                </Typography>
                                                {/*<Typography id="modal-modal-description" sx={{mt: 2}}>*/}
                                                {/*    Duis mollis, est non commodo luctus, nisi erat porttitor ligula.*/}
                                                {/*</Typography>*/}
                                                <h4>Table Name: {formData.name}</h4>
                                                <h4>Rows: {formData.name}</h4>
                                                <h4>Columns: {formData.name}</h4>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked
                                                              onChange={handleBoxChange}/> Show Column Totals at Bottom
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Column Totals at Top
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Row Totals on Left
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Row Totals at Right
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of Row
                                                    Totals
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of Column
                                                    Totals
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Percent of All
                                                </div>
                                                <div>
                                                    <Checkbox label={label} defaultUnchecked/> Show Counts
                                                </div>
                                            </Box>
                                        </Modal>
                                    </div>
                                    :
                                    getError ?
                                        'Unable to process query. Please try again.'
                                        :
                                        ''
                                :
                                ''}</div>}
                    </header>
                </div>

                <div id='right' className={rightOpen}>
                    <div className='icon'
                         onClick={toggleSidebar}>
                        &equiv;
                    </div>
                    <div className='bordered'>
                        <div className={`sidebar ${rightOpen}`}>
                            <div className='header'>
                                <h2 className='titleRight'>
                                    Commands
                                </h2>
                            </div>
                            <div className='leftContent'>
                                <div className='header'>
                                    <h3 className='item'>
                                        Analytical Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Count
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Average
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Cross
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Compare
                                    </p>
                                </div>
                                <div className='header'>
                                    <h3 className='item'>
                                        Selection Commands
                                    </h3>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Breakout By
                                    </p>
                                </div>
                                <div className='header'>
                                    <p className='item'>
                                        Where / Filter
                                    </p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

export default App;